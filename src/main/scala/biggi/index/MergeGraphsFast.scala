package biggi.index

import java.io.File
import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.{Direction}
import scala.collection.JavaConversions._
import org.apache.commons.logging.LogFactory
import biggi.util.BiggiFactory
import com.tinkerpop.blueprints.util.wrappers.batch.{BatchGraph, VertexIDType}

/**
 * @author dirk
 *          Date: 8/23/13
 *          Time: 10:33 AM
 */
object MergeGraphsFast {

    private final val LOG = LogFactory.getLog(getClass)

    def main(args:Array[String]) {
        val inDir = new File(args(0))
        val outDir = new File(args(1))

        val _override = true

        val newGraph = true

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
        //titanConf.setProperty("storage.transactions","false")

        val bigGraph = TitanFactory.open(titanConf)
        if(newGraph) {
            BiggiFactory.initGraph(bigGraph)
        }

        val bGraph = new BatchGraph(bigGraph, VertexIDType.STRING, 1000)

        LOG.info("Merging into: "+ outDir.getAbsolutePath)

        inDir.listFiles().foreach(indexDir => {
            LOG.info("Merging: "+indexDir.getAbsolutePath)
            val smallConf = BiggiFactory.getGraphConfiguration(indexDir)
            smallConf.setProperty("storage.transactions","false")
            smallConf.setProperty("storage.read-only","true")
            val smallGraph = TitanFactory.open(smallConf)

            smallGraph.query().vertices().iterator().foreach(fromSmall => {
                val fromCui = fromSmall.getProperty[String]("cui")

                var from = bGraph.getVertex(fromCui)

                if(from == null) {
                    val fromSemTypes = fromSmall.getProperty[String]("semtypes")
                    from = bGraph.addVertex(fromCui,"cui", fromCui,"semtypes", fromSemTypes)
                }

                fromSmall.getEdges(Direction.OUT).iterator().foreach(e => {
                    val toSmall = e.getVertex(Direction.IN)
                    if(!fromSmall.getId.equals(toSmall.getId)) {
                        val toCui = toSmall.getProperty[String]("cui")

                        val newIds = e.getProperty[String]("uttIds")
                        val count = e.getProperty[java.lang.Integer]("count")
                        val rel = e.getLabel

                        var to = bGraph.getVertex(toCui)

                        if(to == null) {
                            val toSemtypes = toSmall.getProperty[String]("semtypes")
                            to = bGraph.addVertex(toCui,"cui", toCui,"semtypes", toSemtypes)
                        }


                        val edge = bGraph.addEdge(null, from, to, rel)
                        edge.setProperty("uttIds", newIds)
                        edge.setProperty("count", count)
                    }
                })
            })
            smallGraph.shutdown()
            bGraph.commit()
        })
        bGraph.shutdown()
        LOG.info("DONE!")
        System.exit(0)
    }
}
