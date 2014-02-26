import biggi.util.BiggiUtils._
import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.{Direction, Edge}
import java.io.{FileWriter, PrintWriter, File}
import scala.io.Source
import scala.collection.JavaConversions._
import com.tinkerpop.gremlin.scala._
import scala.util.Random

val graphDir = new File("/ssd/data/umlsGraph")

val conf = getGraphConfiguration(graphDir)
conf.addProperty("storage.transactions",false)


val g = TitanFactory.open(conf)





val cuiId = getCuiToID(graphDir)

val selected = Source.fromFile("/ssd/data/selectedVertices").getLines().toSet


var pairs = Set[(String,String)]()

var setA = Set[String]()
var setB = Set[String]()
Source.fromFile("/ssd/data/protein_protein_biogrid.tsv").getLines().foreach(line => {
    val Array(conceptA,conceptB) = line.split("\t",2)
    if(selected.contains(conceptA) && selected.contains(conceptB) && !pairs.contains((conceptA,conceptB))) {
        setA += conceptA
        setB += conceptB
        /*val fromId = cuiId.get(conceptA)
        val toId = cuiId.get(conceptB)

        val from = if(fromId == null) {
            val v = g.addVertex()
            v.setProperty(UI,conceptA)
            cuiId.put(conceptA,v.getId.asInstanceOf[java.lang.Long])
            v
        } else g.getVertex(fromId)
        val to = if(toId == null) {
            val v = g.addVertex()
            v.setProperty(UI,conceptB)
            cuiId.put(conceptB,v.getId.asInstanceOf[java.lang.Long])
            v
        } else g.getVertex(toId)

        val edge = g.addEdge(null,from,to,EDGE)
        edge.setProperty(LABEL,"interacts_with")
        edge.setProperty(SOURCE,"biogrid")*/

        pairs += ((conceptA,conceptB))
    }
})/*

g.commit()
g.shutdown()

saveCuiToID(graphDir,cuiId)
*/

setA.size
setB.size
pairs.size
val originalA = setA.foldLeft(Set[String]())(_ + _)


def filter(concepts:Set[String]) {
    concepts.toList.foreach(ui => {
        var tooNarrow = false
        g.query.has(UI,ui).vertices().head.outE().sideEffect((e:Edge)=> tooNarrow = tooNarrow || {
            val l = e.getProperty[String](LABEL)
            (l == "CHD" || l == "RN") && {
                originalA.contains(e.getVertex(Direction.IN).getProperty[String](UI))
            }
        }).toList
    
        if(tooNarrow)
            setA -= ui
    })
}
//filter(setA)
//filter(setB)

setA.size
setB.size
val oldAs = Array[String](setA.toSeq:_*)
val oldBs = Array[String](setB.toSeq:_*)

var s = 0
var pw = new PrintWriter(new FileWriter(new File("/ssd/data/pos_p2p.csv")))
pairs.foreach{ case (conceptA,conceptB) =>
    if(setA.contains(conceptA) && setB.contains(conceptB)) {
        setA -= conceptA
        setB -= conceptB
        pw.println(s"$conceptA,$conceptB")
        s += 1
    }
}
pw.close()
println("Written pos ex")

val bArray = oldBs.toArray
setB = oldBs.toSet
val zip = oldAs.zip(Random.shuffle(oldBs)).flatMap(pair => {
    if(pairs.contains(pair))
        None
    else
        Some(pair)
})

pw = new PrintWriter(new FileWriter(new File("/ssd/data/neg_p2p.csv")))
zip.foreach{ case (conceptA,conceptB) =>
    pw.println(s"$conceptA,$conceptB")
}
pw.close()
println("done")
System.exit(0)







