package biggi.inference

import scala.collection.JavaConversions._
import scala.collection.mutable
import com.tinkerpop.rexster.client.{RexsterClientTokens, RexsterClient, RexsterClientFactory}
import scala.collection.parallel.immutable.ParSeq
import scala.collection.parallel.mutable.ParArray
import org.apache.commons.configuration.BaseConfiguration


/**
 * @author dirk
 *          Date: 10/9/13
 *          Time: 3:31 PM
 */
object ExtractInterestingPathsFromRexster {

    def main(args:Array[String]) {
        val hosts = args(0)
        val fromCui = args(1)
        val toCui = args(2)
        val maxLength = args(3).toInt
        val maxResults = args(4).toInt

        def getId(client: RexsterClient,cui:String): Long = {
            val result = client.execute[Long](s"v = g.V('cui','$cui') \n v.id")
            if(result != null && result.isEmpty)
                -1
            else
                result.head
        }

        val graphClients = hosts.split(",").map(h => {
            val Array(host,port,graph) = h.split(":",3)
            val conf = new BaseConfiguration() {
                addProperty(RexsterClientTokens.CONFIG_GRAPH_NAME,graph)
                addProperty(RexsterClientTokens.CONFIG_HOSTNAME,host)
                addProperty(RexsterClientTokens.CONFIG_PORT,port)
                addProperty(RexsterClientTokens.CONFIG_TIMEOUT_CONNECTION_MS, 30000)
                addProperty(RexsterClientTokens.CONFIG_TRANSACTION, "true")
            }

            RexsterClientFactory.open(conf)
        }).toSeq.zipWithIndex.par.toParArray

        //cui to ids
        val mapping = new mutable.HashMap[String,Array[Long]] with mutable.SynchronizedMap[String,Array[Long]]

        mapping += fromCui -> graphClients.map(client => getId(client._1,fromCui)).toArray
        mapping += toCui -> graphClients.map(client => getId(client._1,toCui)).toArray

        type CEdge = (String, String, String)
        type CVertexId = String

        val priorityQueue =
            new mutable.SynchronizedPriorityQueue[(List[Either[CVertexId,CEdge]], Double)]()(new Ordering[(List[Either[CVertexId,CEdge]], Double)]{
                def compare(x: (List[Either[CVertexId,CEdge]], Double), y: (List[Either[CVertexId,CEdge]], Double)) =
                    math.signum(y._2 - x._2).toInt
            })

        priorityQueue.enqueue((List(Left(fromCui)),0.0))
        var result = List[List[Either[CVertexId,CEdge]]]()

        val start = System.currentTimeMillis()
        var time:Long = 0
        val maxTime = 60*1000

        while(!priorityQueue.isEmpty && result.size < maxResults && time < maxTime) {
            val (partialPath, score) = priorityQueue.dequeue()
            val lastCui = partialPath.head.left.get

            if(lastCui.equals(toCui)) {
                result ::= partialPath.reverse
                var prevVertex = ""
                println(result.head.map{
                    case Left(cui) => prevVertex = cui; cui
                    case Right(edge) => edge._2 + { if (edge._3 == prevVertex) "^-1" else "" }
                }.reduce(_+" , "+_))
            }
            else if(partialPath.size / 2 < maxLength) {

                def addEdge(candCui: String, client: (RexsterClient, Int), candId: Long, label: String, outE:Boolean) {
                    if(!mapping.contains(candCui))
                        mapping += candCui -> graphClients.map(client => getId(client._1,candCui)).toArray

                    if (allowEdge(label) && !partialPath.exists(v => v.left.getOrElse("") == candCui)) {
                        val eScore = scoreEdge(label, mapping(candCui), !outE, graphClients)
                        priorityQueue.enqueue(
                            (List(Left(candCui), if(outE) Right((lastCui, label, candCui)) else Right((candCui, label, lastCui))) ++
                                partialPath, eScore + score))
                    }
                }

                //look at outEdges
                graphClients.foreach(client => {
                    val fromId = mapping(lastCui)(client._2)
                    if(fromId != -1)
                        client._1.execute[java.util.ArrayList[Any]](s"g.v($fromId).outE.transform{[it.label,it.inV.next().id,it.inV.next().cui]}").foreach {
                            case list => {
                                val label = list(0).asInstanceOf[String];val candId=list(1).asInstanceOf[Long];val candCui=list(2).asInstanceOf[String]
                                addEdge(candCui, client, candId, label, true)
                            }
                        }
                })

                //look at inEdges
                graphClients.foreach(client => {
                    val fromId = mapping(lastCui)(client._2)
                    if(fromId != -1)
                        client._1.execute[java.util.ArrayList[Any]](s"g.v($fromId).inE.transform{[it.label,it.outV.next().id,it.outV.next().cui]}").foreach {
                            case list => {
                                val label = list(0).asInstanceOf[String];val candId=list(1).asInstanceOf[Long];val candCui=list(2).asInstanceOf[String]
                                addEdge(candCui, client, candId, label, false)
                            }
                        }
                })
            }
            time = System.currentTimeMillis() - start
        }

        priorityQueue.foreach {
            case (path,score) => {
                if(path.head.left.getOrElse("").equals(toCui)) {
                    result ::= path.reverse
                    var prevVertex = ""
                    println(result.head.map {
                        case Left(cui) => prevVertex = cui; cui
                        case Right(edge) => edge._2 + { if (edge._3 == prevVertex) "^-1" else "" }
                    }.reduce(_+" , "+_))
                }
            }
        }

        println("Query time: "+time+" milliseconds")

       /* result.foreach(p => println(p.map(_ match {
            case v:Vertex => prevVertex = v; v.getProperty("cui")
            case e:Edge => e.getLabel + { if (e.getVertex(Direction.IN).getId == prevVertex.getId) "^-1" else "" }
        }).reduce(_+" , "+_)))  */
        graphClients.foreach(_._1.close())
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

    def scoreEdge(eLabel:String, vertexIds:Array[Long],out:Boolean,clients:ParArray[(RexsterClient,Int)]) = {
        clients.map(client => {
            val dir = if(out) "outE" else "inE"
            val id = vertexIds(client._2)
            if(id == -1)
                0
            else {
                val exec: String = s"""g.v($id).$dir("$eLabel").count()"""
                client._1.execute[Long](exec).head
            }
        }).sum
    }
    
    def allowEdge(label:String) = !notAllowedEdges.contains(label)

    private val notAllowedEdges = List("same_as","isa","may_be_a","clinically_similar","has_tradename","")
}
