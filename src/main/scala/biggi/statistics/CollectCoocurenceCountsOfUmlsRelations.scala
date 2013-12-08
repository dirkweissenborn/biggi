package biggi.statistics

import org.apache.commons.logging.LogFactory
import java.io.{FileWriter, PrintWriter, File}
import biggi.util.BiggiUtils
import com.thinkaurelius.titan.core.TitanFactory
import scala.collection.mutable
import scala.collection.JavaConversions._
import com.tinkerpop.blueprints.Direction
import scala.io.Source

/**
 * @author dirk
 *          Date: 11/4/13
 *          Time: 12:35 PM
 */
object CollectCoocurenceCountsOfUmlsRelations {

    private final val LOG = LogFactory.getLog(this.getClass)

    def main(args:Array[String]) {
        val graphDir = new File(args(0))
        val outFile = new File(args(1))
        outFile.getParentFile.mkdirs()

        val relationsFile = new File(args(2))
        val relation = args(3)

        val conf = BiggiUtils.getGraphConfiguration(graphDir)
        conf.setProperty("storage.transactions","false")
        conf.setProperty("storage.read-only","true")
        val g = TitanFactory.open(conf)

        val map = mutable.HashMap[String,Int]()
        var counter = 1

        val pw = new PrintWriter(new FileWriter(outFile))

        Source.fromFile(relationsFile).getLines.foreach(triple => {
            val Array(toCui,fromCui,rel) = triple.split("\t",3)
            if(rel == relation) {
                val from = {
                    val it = g.query().has(BiggiUtils.UI,fromCui).vertices()
                    if(it.isEmpty)
                        None
                    else
                        Some(it.head)
                }.getOrElse(null)
                if(from != null) {
                    val to = {
                        val it = g.query().has(BiggiUtils.UI,toCui).vertices()
                        if(it.isEmpty)
                            None
                        else
                            Some(it.head)
                    }.getOrElse(null)

                    if(to != null) {
                        from.getEdges(Direction.OUT).filter(_.getVertex(Direction.IN).equals(to)).foreach(edge => {
                            //map.getOrElseUpdate(edge.getLabel,0)
                            //map(edge.getLabel) += 1
                            pw.println(s"$fromCui\t$toCui\t"+edge.getLabel)
                        })

                        from.getEdges(Direction.IN).filter(_.getVertex(Direction.OUT).equals(to)).foreach(edge => {
                            //map.getOrElseUpdate(edge.getLabel+"^-1",0)
                            //map(edge.getLabel+"^-1") += 1
                            pw.println(s"$fromCui\t$toCui\t"+edge.getLabel+"^-1")
                        })

                        counter+=1
                        if(counter % 10000 == 0)
                            LOG.info(counter+" pairs processed")
                    }
                }
            }
        })

        g.shutdown()
       /*
        map.foreach {
            case (label,count) => {
                pw.println(s"$label\t$count")
            }
        } */
        pw.close()

        System.exit(0)
    }

}
