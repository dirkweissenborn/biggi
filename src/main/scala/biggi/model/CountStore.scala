package biggi.model

import java.io.{FileWriter, PrintWriter, File}
import scala.io.Source
import scala.collection.concurrent.TrieMap
import biggi.util.BiggiUtils
import com.thinkaurelius.titan.core.TitanFactory
import scala.collection.JavaConversions._
import biggi.model.deppath.DependencyPath

/**
 * @author dirk
 *          Date: 11/11/13
 *          Time: 10:37 AM
 */
class CountStore {

    private var trie = TrieMap[String,Int]()
    private var totalCount = 0

    def getCount(el:String) = {
        trie.get(el).getOrElse(0)
    }
    
    def contains(el:String) = {
        trie.contains(el)
    }

    def update(el:String, by:Int = 1) = {
        trie += el -> (getCount(el)+by)
    }

    def getTotalCount = totalCount
}

object CountStore {
    def fromFile(countFile:File) = {
        val store = new CountStore

        Source.fromFile(countFile).getLines().foreach(line => {
            val Array(rel,ct) = line.split("\t",2)
            val count = ct.toInt
            store.totalCount += count
            store.trie += rel -> count
        })
        store
    }

    def main(args:Array[String]) = {
        printStatistics
    }

    def printStatistics{
        val pathsFile = new File("/ssd/data/totalEdgeCounts.tsv")
        var all = 0

        var map = Map[String,Int]()

        Source.fromFile(pathsFile).getLines().foreach(line => {
            val Array(path, ct) = line.split("\t", 2)
            try {
                val depPath = DependencyPath.fromString(path).toShortString
                map += depPath -> (map.getOrElse(depPath,0) + ct.toInt)
            }
            catch {
                case e:Throwable => //skip
            }
                all += 1
        })

        println(s"all: ${map.size}")

        val pw = new PrintWriter(new FileWriter(new File("/ssd/data/normalizedTotalEdges.tsv")))
        var ct50 = 0
        var total50 = 0
        var total = 0
        map.foreach(el => {
            pw.println(s"${el._1}\t${el._2}")
            total += el._2
            if(el._2 >= 50) {
                ct50 += 1
                total50 += el._2
            }

        })
        pw.close()

        println(s"total: $total")

        println(s"all over 50: $ct50")
        println(s"total over 50: $total50")
    }

    def evaluateChangeOfCounts {
        val pathsFile = new File("/ssd/data/totalEdgeCounts.tsv")
        val intervals = new Array[Int](100)
        var all = 0
        var total = 0.0
        val pw = new PrintWriter(new FileWriter(new File("/ssd/data/selectedEdges50.tsv")))

        Source.fromFile(pathsFile).getLines().foreach(line => {
            val Array(_, ct) = line.split("\t", 2)
            all += 1
            try {
                val count = ct.toInt
                total += count
                if (count < 1000)
                    intervals(count / 10) += 1
                if (count >= 50)
                    pw.println(line)
            } catch {
                case t: Throwable =>
            }
        })
        pw.close()
        var acc = 0
        (0 until 100).map(i => {
            acc += intervals(i)
            ((i + 1) * 10, intervals(i), if (i == 0) 0 else intervals(i - 1).toDouble / intervals(i))
        }).foreach(inter => println(inter._1 + "\t" + inter._2 + "\t" + inter._3))
        println()
        println("total: " + total)
        println("all: " + all)
        println("avg.: " + (total / all))
    }

    def evaluateCounts {
        val conf = BiggiUtils.getGraphConfiguration(new File("/data/test/mergedGraph1"))
        val graph = TitanFactory.open(conf)
        val conf2 = BiggiUtils.getGraphConfiguration(new File("/data/test/umlsGraph"))
        val umlsGraph = TitanFactory.open(conf2)
        val store = fromFile(new File("/data/totalVertexCounts.tsv"))
        val sortedValues = store.trie.toList.sortBy(-_._2)

        /*umlsGraph.query().has("depth",1).vertices().withFilter(_.getProperty[Integer]("totalDepth") > 1).foreach(v => {
            val cui  = v.getProperty[String]("ui")
            val text = graph.query().has("ui",cui).vertices().foldLeft("")((acc,v) => acc + " " +v.getProperty[String]("text"))
            println(cui+"  " +text)
        }) */
        val pw = new PrintWriter(new FileWriter("/data/relDepth.tsv"))
        sortedValues.foreach(v => {
            val (d,td) = umlsGraph.query().has("ui",v._1).vertices().foldLeft((0,1))((acc,v) => (v.getProperty[Integer]("depth"),v.getProperty[Integer]("totalDepth")))
            if(d > 0 && td > 1)
                pw.println(v._2 +"\t"+d.toDouble/td)
        })
        pw.close()
        println(sortedValues.head._2+"\t\t"+graph.query().has("ui",sortedValues.head._1).vertices().foldLeft("")((acc,v) => acc + " " +v.getProperty[String]("text")))
        sortedValues.take(1000).sliding(3).foreach{
            case List(previous,current,next) =>
                val text = graph.query().has("ui",current._1).vertices().foldLeft("")((acc,v) => acc + " " +v.getProperty[String]("text"))
                val (d,d_td) = umlsGraph.query().has("ui",current._1).vertices().foldLeft((0,0.0))((acc,v) => (v.getProperty[Integer]("depth"),v.getProperty[Integer]("depth").toDouble/v.getProperty[Integer]("totalDepth")))
                println("%s\t\t%d\t\t%1.2f\t\t%d\t\t%1.2f\t\t%s".format(current._1,current._2,current._2.toDouble/previous._2,d,d_td,text))
        }
        graph.shutdown()
        System.exit(0)
    }
}
