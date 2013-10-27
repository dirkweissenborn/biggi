package biggi.statistics

import java.io.{FileWriter, PrintWriter, File}
import biggi.util.BiggiFactory
import com.thinkaurelius.titan.core.TitanFactory
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.apache.commons.logging.LogFactory

/**
 * @author dirk
 *          Date: 10/14/13
 *          Time: 3:00 PM
 */
object CollectEdgeCountsOfGraph {

    private final val LOG = LogFactory.getLog(this.getClass)

    def main(args:Array[String]) {
        val graphDir = new File(args(0))

        val outFile = new File(args(1))


        val conf = BiggiFactory.getGraphConfiguration(graphDir)
        conf.setProperty("storage.transactions","false")
        conf.setProperty("storage.read-only","true")
        val g = TitanFactory.open(conf)

        val map = mutable.HashMap[String,Int]()
        var counter = 1

        g.getEdges.foreach(edge => {
//            if(edge.getProperty[String]("uttIds") != "umls") {
            map.getOrElseUpdate(edge.getLabel,0)
            map(edge.getLabel) += 1
            counter+=1
            if(counter % 100000 == 0)
                LOG.info(counter+" edges processed")
//            }
        })

        g.shutdown()

        val pw = new PrintWriter(new FileWriter(outFile))

        map.foreach {
            case (label,count) => {
                pw.println(s"$label\t$count")
            }
        }
        pw.close()

        System.exit(0)
    }

}
