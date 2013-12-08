package biggi.cluster

import java.io.{PrintWriter, File}
import biggi.util.BiggiUtils._
import com.thinkaurelius.titan.core.{TitanEdge, TitanFactory}
import com.tinkerpop.blueprints.Direction
import scala.collection.JavaConversions._
import breeze.plot._
import scala.Predef._
import scala.util.Random
import breeze.plot.Figure
import breeze.linalg._
import breeze.io.TextWriter.FileWriter

/**
 * @author dirk
 *          Date: 12/5/13
 *          Time: 2:06 PM
 */
object LSAEdges {

    def main(args:Array[String]) {
        val graphDir = new File(args(0))
        val outDir = new File(args(1))
        outDir.mkdirs()
        val conf = getGraphConfiguration(graphDir)
        conf.addProperty("storage.transactions","false")

        val graph = TitanFactory.open(conf)
        val cuiId = getCuiToID(graphDir)
        var edgeMap = Map[String,Term]()
        var docSet = Map[(Long,Long),Vector[Double]]()
        var ctr = 0

        val idxDim: Int = 1000

        println("Collecting edges from graph...")
        cuiId.values().foreach { case fromId:java.lang.Long =>
            graph.getVertex(fromId).getEdges(Direction.OUT).foreach{case edge:TitanEdge =>
                val label = edge.getProperty[String](LABEL).replaceAll("""\(.*\)""","")
                val toId = edge.getVertex(Direction.IN).getID
                val entry = if(fromId < toId) (fromId.longValue(),toId) else (toId,fromId.longValue())

                val term =
                    if(edgeMap.contains(label)) {
                        val term = edgeMap(label)
                        term.docs += entry
                        term
                    }
                    else {
                        val idxVector = SparseVector.zeros[Double](idxDim)
                        (0 until 10).foreach(_ => idxVector.update(Random.nextInt(idxDim), Random.nextInt(2) * 2 - 1))
                        val term = Term(label, Set(entry), idxVector, SparseVector.zeros[Double](idxDim))
                        edgeMap += label -> term
                        term
                    }

                if(!docSet.contains(entry))
                    docSet = docSet.updated(entry, term.idxVector)
                else
                    docSet = docSet.updated(entry, docSet(entry) + term.idxVector)
            }
            ctr += 1
            if(ctr % 10000 == 0)
                println(s"$ctr vertices processed")
        }
        graph.shutdown()


        val pw = new PrintWriter(new java.io.FileWriter(new File("vectors.arff")))

        pw.println("@RELATION dep_paths")
        (0 until idxDim).foreach(i => pw.println(s"@ATTRIBUTE $i  NUMERIC"))
        pw.println("@ATTRIBUTE dep_path string")

        pw.println("@DATA")

        edgeMap.values.foreach(term => {
            term.ctxtVector = term.docs.foldLeft(term.ctxtVector)((acc,id) => acc + docSet(id))
            term.ctxtVector /= term.ctxtVector.norm(2.0)

            pw.println("{"+term.ctxtVector.iterator.map(value => value._1+" "+value._2).mkString(",")+","+idxDim +" '"+term.label+"'}")
        })
        pw.close()
        System.exit(0)
    }

    protected case class Term(label:String,var docs:Set[(Long,Long)],idxVector:Vector[Double],var ctxtVector:Vector[Double])

}
