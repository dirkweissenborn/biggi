import biggi.model.deppath.GraphPathStore
import biggi.util.BiggiUtils
import com.thinkaurelius.titan.core.TitanFactory
import java.io.File
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.util.Random
val conf = BiggiUtils.getGraphConfiguration(new File("/data/graphs/bigGraph"))
val uiId = BiggiUtils.getCuiToID(new File("/data/graphs/bigGraph"))
val graph = TitanFactory.open(conf)




var total = 0
val counts = mutable.Map[String,Int]()
new File("/data/neg_may_treat_paths").listFiles().foreach(storeFile => {
    val store = GraphPathStore.fromFile(storeFile)
    store.getPaths.foreach(p => {
        if(p.drop(1).dropRight(1).exists(cui => {
            graph.getVertex(uiId.get(cui)).getProperty[String](BiggiUtils.TYPE).contains("aapp")
        })) {
            total += 1
            p.sliding(2).foldLeft(List[String](""))((acc,pair) => {
                store.getEdges(pair(0),pair(1)).keys.toList.flatMap(el => {
                    acc.mapConserve(e => e + " | "+ el.replaceAll("""\([^)]*\)+""",""))
                })
            }).toList.foreach(path => counts += (path -> (counts.getOrElse(path,0)+1)))
        }
    })
})
counts.toSeq.sortBy(-_._2).take(100).foreach(el => println(el._1+"\t"+el._2))



































































































println(counts.size)
println(total)

graph.shutdown()

System.exit(0)


