import biggi.util.BiggiUtils._
import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.{Direction, Edge}
import java.io.{FileWriter, PrintWriter, File}
import scala.io.Source
import scala.collection.JavaConversions._
import com.tinkerpop.gremlin.scala._
import scala.util.Random

val conf = getGraphConfiguration(new File("/data/umlsGraph"))
conf.addProperty("storage.transactions",false)

val g = TitanFactory.open(conf)





var pairs = Set[(String,String)]()

var drugSet = Set[String]()
var diseaseSet = Set[String]()
Source.fromFile("/data/may_treat_pairs.csv").getLines().foreach(line => {
    val Array(drug,disease,_) = line.split(",",3)
    drugSet += drug
    diseaseSet += disease
    pairs += ((drug,disease))
})

drugSet.size
diseaseSet.size

val originalDrugs = drugSet.foldLeft(Set[String]())(_ + _)

def filter(concepts:Set[String]) {
    concepts.toList.foreach(ui => {
        var tooNarrow = false
        g.query.has(UI,ui).vertices().head.outE().sideEffect((e:Edge)=> tooNarrow = tooNarrow || {
            val l = e.getProperty[String](LABEL)
            (l == "CHD" || l == "RN") && {
                originalDrugs.contains(e.getVertex(Direction.IN).getProperty[String](UI))
            }
        }).toList
    
        if(tooNarrow)
            drugSet -= ui
    })
}

filter(drugSet)
filter(diseaseSet)

drugSet.size
diseaseSet.size

val oldDrugSet = Array[String](drugSet.toSeq:_*)
val oldDiseaseSet = Array[String](diseaseSet.toSeq:_*)

var s = 0

var pw = new PrintWriter(new FileWriter(new File("/data/filtered_may_treat_pairs.csv")))
pairs.foreach{ case (drug,disease) =>
    if(drugSet.contains(drug) && diseaseSet.contains(disease)) {
        drugSet -= drug
        diseaseSet -= disease
        pw.println(s"$drug,$disease")
        s += 1
    }
}
pw.close()

println("Written pos ex")

val zip = Random.shuffle(oldDrugSet.toIterator).take(s).map(drug => {
    var dis = oldDiseaseSet(Random.nextInt(oldDiseaseSet.length))
    while(pairs.contains((drug,dis)))
        dis = oldDiseaseSet(Random.nextInt(oldDiseaseSet.length))
    (drug,dis)
})


pw = new PrintWriter(new FileWriter(new File("/data/neg_filtered_may_treat_pairs.csv")))
zip.foreach{ case (drug,disease) =>
    pw.println(s"$drug,$disease")
}
pw.close()

println("done")
System.exit(0)







