package biggi.inference

import java.io.File
import biggi.util.BiggiFactory
import com.thinkaurelius.titan.core.TitanFactory
import scala.collection.JavaConversions._
import scala.collection.mutable
import com.tinkerpop.blueprints.{Element, Edge, Vertex, Direction}


/**
 * @author dirk
 *          Date: 10/9/13
 *          Time: 3:31 PM
 */
object ExtractInterestingPathsFromGraph {

    def main(args:Array[String]) {
        val graphDir = new File(args(0))
        val cui1 = args(1)
        val cui2 = args(2)
        val maxLength = args(3).toInt
        val maxResults = args(4).toInt

        val conf = BiggiFactory.getGraphConfiguration(graphDir)
        conf.setProperty("storage.transactions","false")

        val graph = TitanFactory.open(conf)

        val from = graph.query().has("cui",cui1).limit(1).vertices().head
        val to = graph.query().has("cui",cui2).limit(1).vertices().head

        val priorityQueue =
            new mutable.PriorityQueue[(List[Element], Double)]()(new Ordering[(List[Element], Double)]{
                def compare(x: (List[Element], Double), y: (List[Element], Double)) = math.signum(y._2 - x._2).toInt
            })

        priorityQueue.enqueue((List(from),0.0))
        var result = List[List[Element]]()

        val start = System.currentTimeMillis()
        var time:Long = 0
        val maxTime = 60*1000*10

        while(!priorityQueue.isEmpty && result.size < maxResults && time < maxTime) {
            val (partialPath, score) = priorityQueue.dequeue()
            val lastVertex = partialPath.head.asInstanceOf[Vertex]

            if(lastVertex.getId.equals(to.getId)) {
                result ::= partialPath.reverse
                var prevVertex :Vertex = null
                println(result.head.map(_ match {
                    case v:Vertex => prevVertex = v; v.getProperty("cui")
                    case e:Edge => e.getLabel + { if (e.getVertex(Direction.IN).getId == prevVertex.getId) "^-1" else "" }
                }).reduce(_+" , "+_))
            }
            else if(partialPath.size / 2 < maxLength) {
                lastVertex.getEdges(Direction.OUT).foreach(edge => {
                    if( allowEdge(edge)&& !partialPath.exists(v => v.getId == edge.getVertex(Direction.IN).getId)) {
                        val (cand, eScore) = scoreEdge(edge,Direction.IN)
                        priorityQueue.enqueue((List(cand,edge) ++ partialPath,score+eScore))
                    }
                })

                lastVertex.getEdges(Direction.IN).foreach(edge => {
                    if( allowEdge(edge) && !partialPath.exists(v => v.getId == edge.getVertex(Direction.OUT).getId)) {
                        val (cand, eScore) = scoreEdge(edge,Direction.OUT)
                        priorityQueue.enqueue((List(cand,edge) ++ partialPath,score+eScore))
                    }
                })
            }
            time = System.currentTimeMillis() - start
        }

        priorityQueue.foreach {
            case (path,score) => {
                if(path.head.asInstanceOf[Vertex].getId.equals(to.getId)) {
                    result ::= path.reverse
                    var prevVertex :Vertex = null
                    println(result.head.map(_ match {
                        case v:Vertex => prevVertex = v; v.getProperty("cui")
                        case e:Edge => e.getLabel + { if (e.getVertex(Direction.IN).getId == prevVertex.getId) "^-1" else "" }
                    }).reduce(_+" , "+_))
                }
            }
        }

        println("Query time: "+time+" milliseconds")

       /* result.foreach(p => println(p.map(_ match {
            case v:Vertex => prevVertex = v; v.getProperty("cui")
            case e:Edge => e.getLabel + { if (e.getVertex(Direction.IN).getId == prevVertex.getId) "^-1" else "" }
        }).reduce(_+" , "+_)))  */

        graph.shutdown()
        System.exit(0)

        /*
        val paths = from.->.as("x").bothE().
            filter((e:Edge) =>  e.getLabel != "isa" && e.getLabel != "same_as").
            bothV.
            loop("x",
                (it:LoopBundle[Vertex]) => {it.getLoops <= maxLength && it.getObject.id != to.id && it.getObject.id != from.id && !it.getPath.contains(it.getObject)},
                (it:LoopBundle[Vertex]) => it.getObject.id == to.id).
                path({v:Vertex => v("cui")}, {e:Edge => e.getLabel}).toScalaList()

        paths.foreach(path => println(path.map(_.toString).reduce(_ + "," + _ )))     */
    }

    def scoreEdge(edge:Edge,direction:Direction) = {
        val label = edge.getLabel

        val candidate = edge.getVertex(direction)

        (candidate, candidate.getEdges(direction,label).size)
    }
    
    def allowEdge(e:Edge) = !notAllowedEdges.contains(e.getLabel)

    private val notAllowedEdges = List("same_as","isa","may_be_a","clinically_similar","has_tradename","")
}
