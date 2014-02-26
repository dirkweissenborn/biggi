import biggi.cluster.ClusterEdges._
import java.io.FileInputStream
import scala.collection.mutable
import java.io._
import cc.factorie.la._

var counts = List[Double]()

var ctr = 0
var size = 0
val everyNth = 1000
val result = scala.io.Source.fromFile("/ssd/data/sortedEdgeCounts.tsv").getLines().withFilter(_=>{
    ctr += 1
    ctr % everyNth == 1
}).map(line => {
            val Array(_,ct) = line.split("\t",2)
            val count = ct.toDouble
            size += 1
            count
        }).toList
val xs = (1 to size).toList.map(_.toDouble)
val f = breeze.plot.Figure()
f.height= (f.width * 0.7).toInt
val p = f.subplot(0)
p += breeze.plot.plot(xs,result)
p += breeze.plot.plot(List(1,size),List(50,50),colorcode = "black")
p.ylabel = "number of label occurrences"
//p.xlabel = "number of occurrences"
//p.logScaleX = true
p.logScaleY = true
p.xaxis.setTickLabelsVisible(false)

f.saveas("/home/bioinf/dirkw/Dropbox/Diplom/0.1/images/edgeCount.png",300)

/*deserialize(new FileInputStream("/home/bioinf/dirkw/clusters0.0001_0.0001/clusteredDepPaths.bin"))

/*(0 until numTopics).foreach(topic => {
            val pw = new PrintWriter(new FileWriter(new File("/home/bioinf/dirkw/clusters0.001_0.001","topic %d-%1.3f".format(topic, topicProbs(topic)))))
            val paths = phis(topic).value.masses.toArray.zipWithIndex.sortBy(-_._1).take(100)
            val total = phis(topic).value.massTotal
            paths.foreach(el => pw.println(WordDomain.category(el._2)+"\t"+(el._1/total)))
            pw.close()
        })*/


val relation = "may_treat"

val topGivenRels = getTopicGivenRel

val vec = new DenseTensor1(topGivenRels(WordDomain.index(relation)).toArray)

val nr = 100

val queue = new mutable.PriorityQueue[(String,Double)]()(new Ordering[(String,Double)]{
    def compare(x: (String, Double), y: (String, Double)): Int = math.signum(x._2 - y._2).toInt
})

var ctr = 0
var entropy = 0.0
WordDomain.indices.foreach(idx => {
    val rel = WordDomain.category(idx)
    val relVec = topGivenRels(idx)

    entropy += relVec.entropy

    val dist = (vec - relVec).twoNormSquared

    queue.enqueue((rel,dist))

    if(ctr > nr)
        queue.dequeue()

    ctr += 1
    if(ctr % 10000==0)
        println(ctr+" processed")
})
println(entropy)


queue.dequeueAll.sortBy(rel => {
    val idx = WordDomain.getIndex(rel._1)
    -(0 until phis.size).map(t => phis(t).value(idx)*topicProbs(t)).sum
}).foreach {
    case (rel,dist) => println(rel)
}
*/


















































