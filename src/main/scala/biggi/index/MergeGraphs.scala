package biggi.index

import java.io.{FileWriter, PrintWriter, File}
import com.thinkaurelius.titan.core.{TitanGraph, TitanFactory}
import com.tinkerpop.blueprints.{Vertex, Graph, Direction}
import scala.collection.JavaConversions._
import com.tinkerpop.blueprints.Query.Compare
import org.apache.commons.logging.LogFactory
import biggi.util.BiggiFactory
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

/**
 * @author dirk
 *          Date: 8/23/13
 *          Time: 10:33 AM
 */
object MergeGraphs {

    private final val LOG = LogFactory.getLog(getClass)

    private final var cuiIdMap =  Map[String,AnyRef]()

    def main(args:Array[String]) {
        val inDir = new File(args(0))
        val outDir = new File(args(1))
        val ignoreDuplicates = args(1) == "true"

        val _override = if(args.size > 3)
            args(3) == "override"
        else
            false

        val newGraph = outDir.mkdirs() || _override || outDir.list().isEmpty

        if(_override){
            println("Overriding output directory!")
            def deleteDir(dir:File) {
                dir.listFiles().foreach(f => {
                    if (f.isDirectory) {
                        deleteDir(f)
                        f.delete()
                    }
                    else
                        f.delete()
                })
            }
            deleteDir(outDir)
        }

        val titanConf = BiggiFactory.getGraphConfiguration(outDir)
        titanConf.setProperty("storage.buffer-size","2048")
        titanConf.setProperty("ids.block-size","600000")
        titanConf.setProperty("storage.transaction",false)
        titanConf.setProperty("storage.batch-loading",true)

        val bigGraph = TitanFactory.open(titanConf)
        if(newGraph) {
           BiggiFactory.initGraph(bigGraph)
        } else
            bigGraph.getVertices.foreach(vertex => cuiIdMap += vertex.getProperty[String](BiggiFactory.UI) -> vertex.getId)

        { //write configuration
            val pw = new PrintWriter(new FileWriter(new File(outDir,"graph.config")))
            pw.println(BiggiFactory.printGraphConfiguration(outDir))
            pw.close()
        }

        LOG.info("Merging into: "+ outDir.getAbsolutePath)

        val files = inDir.listFiles().iterator

        def getGraphVertices(indexDir:File) = Future {
            val smallConf = BiggiFactory.getGraphConfiguration(indexDir)
            smallConf.setProperty("storage.transactions","false")
            //smallConf.setProperty("storage.read-only","true")
            val smallGraph = TitanFactory.open(smallConf)

            (smallGraph,smallGraph.getVertices.iterator(),indexDir)
        }

        var nextGraphVertices = getGraphVertices(files.next())
        while(nextGraphVertices != null){
            try {
                val (smallGraph,vertices,indexDir) = Await.result(nextGraphVertices,Duration.Inf)
                if(files.hasNext)
                    nextGraphVertices = getGraphVertices(files.next())
                else
                    nextGraphVertices = null

                var counter = 0

                LOG.info("Merging from: "+ indexDir.getAbsolutePath)

                vertices.foreach(fromSmall => {
                    val fromCui = fromSmall.getProperty[String](BiggiFactory.UI)

                    val fromId = cuiIdMap.get(fromCui)

                    val from =
                        if(fromId.isDefined)
                            bigGraph.getVertex(fromId.get)
                        else {
                            val v = addVertex(newGraph, bigGraph, fromCui, fromSmall)
                            cuiIdMap += fromCui -> v.getId
                            v
                        }

                    var smallEdges = fromSmall.getEdges(Direction.OUT).iterator().toList.groupBy(e => {
                        if(e.getLabel != BiggiFactory.EDGE)
                            (e.getLabel,e.getVertex(Direction.IN).getProperty[String](BiggiFactory.UI))
                        else
                            (e.getProperty[String](BiggiFactory.LABEL),e.getVertex(Direction.IN).getProperty[String](BiggiFactory.UI))
                    } )

                    if(!ignoreDuplicates)
                        from.getEdges(Direction.OUT).foreach(bigEdge => {
                            val key = (bigEdge.getProperty[String](BiggiFactory.LABEL), bigEdge.getVertex(Direction.IN).getProperty[String](BiggiFactory.UI))
                            smallEdges.get(key) match {
                                case Some(edges) => {
                                    bigEdge.setProperty(BiggiFactory.SOURCE,(bigEdge.getProperty[String](BiggiFactory.SOURCE).split(",") ++ edges.flatMap(_.getProperty[String](BiggiFactory.SOURCE).split(","))).toSet.mkString(","))
                                    smallEdges -= key
                                }
                                case None =>
                            }
                        } )

                    smallEdges.foreach {
                        case ((label,toCui),smallEdges) => {
                            val toId = cuiIdMap.get(toCui)

                            val to =
                                if(toId.isDefined)
                                    bigGraph.getVertex(toId.get)
                                else {
                                    val v = addVertex(newGraph, bigGraph, toCui, smallEdges.head.getVertex(Direction.IN))
                                    cuiIdMap += toCui -> v.getId
                                    v
                                }

                            val e = bigGraph.addEdge(null,from,to,BiggiFactory.EDGE)
                            smallEdges.foreach(smallEdge => smallEdge.getPropertyKeys.foreach(key => e.setProperty(key,smallEdge.getProperty(key))))
                            e.setProperty(BiggiFactory.SOURCE, smallEdges.flatMap(_.getProperty[String](BiggiFactory.SOURCE).split(",")).toSet.mkString(","))
                            e.setProperty(BiggiFactory.LABEL, label)
                        }
                    }

                    counter += 1
                    if(counter % 10000 == 0) {
                        LOG.info(counter+" vertices of "+ indexDir.getName+" processed")
                        bigGraph.commit()
                    }
                })
                Future{smallGraph.shutdown()}
                bigGraph.commit()
            } catch {
                case t:Throwable => LOG.error(t.printStackTrace())
            }
        }
        bigGraph.shutdown()
        LOG.info("DONE!")
        System.exit(0)
    }


    def addVertex(newGraph: Boolean, graph: Graph, cui: String, smallVertex: Vertex): Vertex = {
        val it1 = if (!newGraph) graph.query.has(BiggiFactory.UI, Compare.EQUAL, cui).limit(1).vertices() else null
        if (!newGraph && !it1.isEmpty)
            it1.head
        else {
            val f = graph.addVertex(null)
            f.setProperty(BiggiFactory.UI, cui)
            val fromSemTypes = smallVertex.getProperty[String](BiggiFactory.TYPE)
            if (fromSemTypes ne null)
                f.setProperty(BiggiFactory.TYPE, fromSemTypes)
            f.setProperty(BiggiFactory.TEXT, smallVertex.getProperty[String](BiggiFactory.TEXT))
            f
        }
    }
}
