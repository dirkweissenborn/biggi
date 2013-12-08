package biggi.statistics

import java.io.{FileWriter, PrintWriter, File}
import biggi.util.BiggiUtils
import com.thinkaurelius.titan.core.TitanFactory
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.apache.commons.logging.LogFactory
import com.tinkerpop.blueprints.Direction

/**
 * @author dirk
 *          Date: 10/14/13
 *          Time: 3:00 PM
 */
object CollectVertexCountsOfGraph {

    private final val LOG = LogFactory.getLog(this.getClass)

    def main(args:Array[String]) {
        val graphDir = new File(args(0))
        val outFile = new File(args(1))

        var list = List[(String,Int)]()
        var counter = 1

        val (vertices,g) = {
            val conf = BiggiUtils.getGraphConfiguration(graphDir)
            conf.setProperty("storage.transactions","false")
            conf.setProperty("storage.cache-percentage",1)

            //smallConf.setProperty("storage.read-only","true")
            val graph = TitanFactory.open(conf)

            if(graphDir.list().exists(_.endsWith("ui_id.bin"))) {
                val cuiIdMap = BiggiUtils.getCuiToID(graphDir)
                (cuiIdMap.iterator.map(id => (graph.getVertex(id._2),id._1)),graph)
            }
            else
                (graph.getVertices.iterator().map(v => (v,v.getProperty[String](BiggiUtils.UI))),graph)
        }

        vertices.foreach{ case (vertex,cui) =>
            if(!cui.contains(".")) {
                list = cui -> vertex.getEdges(Direction.BOTH).size :: list
                counter+=1
                if(counter % 10000 == 0)
                    LOG.info(counter+" vertices processed")
            }
        }

        g.shutdown()

        val pw = new PrintWriter(new FileWriter(outFile))

        list.foreach {
            case (label,count) =>
                pw.println(s"$label\t$count")
        }
        pw.close()

        System.exit(0)
    }

}
