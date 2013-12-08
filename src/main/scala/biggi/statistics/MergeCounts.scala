package biggi.statistics

import java.io.{FileWriter, PrintWriter, File}
import scala.io.Source
import scala.collection.mutable
import org.apache.commons.logging.LogFactory

/**
 * @author dirk
 *          Date: 10/14/13
 *          Time: 5:40 PM
 */
object MergeCounts {

    private final val LOG = LogFactory.getLog(this.getClass)

    def main(args:Array[String]) {
        val dir = new File(args(0))
        val outFile = new File(args(1))

        val files = dir.listFiles()

        var map = Map[String,Int]()
        LOG.info("Processing "+files.head.getAbsolutePath)
        Source.fromFile(files.head).getLines().foreach(line => {
            val Array(key,count) = line.split("\t",2)
            map += key -> count.toInt
        })

        files.tail.foreach(file => {
            LOG.info("Processing "+file.getAbsolutePath)
            Source.fromFile(file).getLines().foreach(line => {
                val Array(key,count) = line.split("\t",2)
                val c = map.getOrElse(key,0)
                map += key -> (c+count.toInt)
            })
        })

        val pw = new PrintWriter(new FileWriter(outFile))
        map.foreach {
            case (label,count) =>
                pw.println(s"$label\t$count")
        }
        pw.close()

        System.exit(0)
    }
}
