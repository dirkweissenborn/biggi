package biggi.index

import java.io.File
import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.{Direction}
import scala.collection.JavaConversions._
import com.tinkerpop.blueprints.Query.Compare
import org.apache.commons.logging.LogFactory
import biggi.util.BiggiFactory

/**
 * @author dirk
 *          Date: 8/23/13
 *          Time: 10:33 AM
 */
object MergeGraphs {

    private final val LOG = LogFactory.getLog(getClass)

    def main(args:Array[String]) {
        val inDir = new File(args(0))
        val outDir = new File(args(1))

        val _override = if(args.size > 2)
            args(2) == "override"
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
        //titanConf.setProperty("storage.batch-loading","true")

        val bigGraph = TitanFactory.open(titanConf)
        if(newGraph) {
           BiggiFactory.initGraph(bigGraph)
        }

        LOG.info("Merging into: "+ outDir.getAbsolutePath)

        inDir.listFiles().foreach(indexDir => {
            LOG.info("Merging: "+indexDir.getAbsolutePath)
            val smallConf = BiggiFactory.getGraphConfiguration(indexDir)
            smallConf.setProperty("storage.transactions","false")
            smallConf.setProperty("storage.read-only","true")
            val smallGraph = TitanFactory.open(smallConf)

            smallGraph.query().vertices().iterator().foreach(fromSmall => {
                val fromCui = fromSmall.getProperty[String]("cui")
                val it1 = bigGraph.query.has("cui", Compare.EQUAL, fromCui).limit(1).vertices()
                val from =
                    if (!it1.isEmpty)
                        it1.head
                    else {
                        val f = bigGraph.addVertex(null)
                        f.setProperty("cui", fromCui)
                        val fromSemTypes = fromSmall.getProperty[String]("semtypes")
                        f.setProperty("semtypes", fromSemTypes)
                        f
                    }

                fromSmall.getEdges(Direction.OUT).iterator().foreach(e => {
                    val toSmall = e.getVertex(Direction.IN)
                    if(!fromSmall.getId.equals(toSmall.getId)) {
                        val toCui = toSmall.getProperty[String]("cui")

                        val newIds = e.getProperty[String]("uttIds")
                        val count = e.getProperty[java.lang.Integer]("count")
                        val rel = e.getLabel
                        val it2 = bigGraph.query.has("cui", Compare.EQUAL, toCui).limit(1).vertices()

                        val to = if (!it2.isEmpty)
                            it2.head
                        else {
                            val t = bigGraph.addVertex(null)
                            t.setProperty("cui", toCui)
                            val toSemTypes = toSmall.getProperty[String]("semtypes")
                            t.setProperty("semtypes", toSemTypes)
                            t
                        }

                        from.getEdges(Direction.OUT, rel).find(_.getVertex(Direction.IN) == to) match {
                            case Some(edge) => {
                                try {
                                    val ids = edge.getProperty[String]("uttIds")
                                    if(!ids.contains(newIds)) {
                                        edge.setProperty("count", edge.getProperty[java.lang.Integer]("count") + count)
                                        edge.setProperty("uttIds", ids + "," + newIds)
                                    }
                                }
                                catch {
                                    case e: Throwable => LOG.warn("Could not merge edge: "+ edge.getLabel)
                                }
                            }
                            case None => {
                                val edge = bigGraph.addEdge(null, from, to, rel)
                                edge.setProperty("uttIds", newIds)
                                edge.setProperty("count", count)
                            }
                        }
                    }
                })
            })
            smallGraph.shutdown()
            bigGraph.commit()
        })
        bigGraph.shutdown()
        LOG.info("DONE!")
        System.exit(0)
    }

}
