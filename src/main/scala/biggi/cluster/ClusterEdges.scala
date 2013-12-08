package biggi.cluster

import java.io.{FileWriter, PrintWriter, File}
import biggi.util.BiggiUtils._
import com.thinkaurelius.titan.core.{TitanVertex, TitanEdge, TitanFactory}
import com.tinkerpop.blueprints.Direction
import scala.collection.JavaConversions._
import scala.Predef._
import scala.util.Random
import cc.factorie.app.topics.lda.SparseLDAInferencer
import cc.factorie.directed._
import cc.factorie.variable._
import scala.collection.mutable.ArrayBuffer

/**
 * @author dirk
 *          Date: 12/5/13
 *          Time: 2:06 PM
 */
object ClusterEdges {

    val numTopics = 100
    val beta1 = 0.1
    val alpha1 = 0.1
    val fitDirichlet = false

    var avgNrOfConnections = 0.0

    implicit val model = DirectedModel()
    object ZDomain extends DiscreteDomain(numTopics)
    object ZSeqDomain extends DiscreteSeqDomain { def elementDomain = ZDomain }
    class Zs(len:Int) extends DiscreteSeqVariable(len) {
        def domain = ZSeqDomain
    }
    object WordSeqDomain extends CategoricalSeqDomain[String]
    val WordDomain = WordSeqDomain.elementDomain
    WordDomain.gatherCounts = true
    class Document(name:String, myTheta:ProportionsVariable, myZs:Zs, words:Seq[String]) extends cc.factorie.app.topics.lda.Document(WordSeqDomain, name, words) {
        words.foreach(w => WordDomain.index(w))
        this.theta = myTheta
        this.zs = myZs
        avgNrOfConnections += words.size
    }
    val beta = MassesVariable.growableUniform(WordDomain, beta1)
    val alphas = MassesVariable.dense(numTopics, alpha1)

    def main(args:Array[String]) {
        implicit val random = new Random(0)
        val graphDir = new File(args(0))
        val outDir = new File(args(1))
        outDir.mkdirs()
        val conf = getGraphConfiguration(graphDir)
        conf.addProperty("storage.transactions","false")

        val graph = TitanFactory.open(conf)
        val cuiId = getCuiToID(graphDir)
        var ctr = 0

        val phis = Mixture(numTopics)(ProportionsVariable.growableDense(WordDomain) ~ Dirichlet(beta))
        val documents = new ArrayBuffer[Document]

        println("Collecting edges from graph...")
        cuiId.values().foreach { case id:java.lang.Long =>
            graph.getVertex(id).getEdges(Direction.BOTH).groupBy(e => (e.getVertex(Direction.OUT),e.getVertex(Direction.IN))).foreach{
                case ((from:TitanVertex,to:TitanVertex),edges:Iterable[TitanEdge]) =>
                    if(from.getID < to.getID) {
                        val tokens = edges.flatMap(edge => {
                            val label = edge.getProperty[String](LABEL)
                            val token = {if("""(:neg)|(no[^a-z])""".r.findFirstIn(label) != None) "neg_" else "" } +label.replaceAll("""\(.*\)""","")
                            val nr = edge.getProperty[String](SOURCE).count(_ == ',') +1
                            for(i <- 0 until nr) yield token
                        }).toSeq

                        if(tokens.tail.exists(_ != tokens.head)){
                            val theta = ProportionsVariable.sortedSparseCounts(numTopics) ~ Dirichlet(alphas)
                            val zs = new Zs(tokens.length) :~ PlatedDiscrete(theta)
                            documents += new Document(from.getProperty[String](UI) + to.getProperty[String](UI), theta, zs, tokens) ~ PlatedCategoricalMixture(phis, zs)
                        }
                    }
                }
            ctr += 1
            if(ctr % 10000 == 0)
                println(s"$ctr vertices processed")
        }
        graph.shutdown()
        val _size = documents.size
        println("Avg. nr of connections: "+(avgNrOfConnections / _size))
        println("Nr of concept pairs: "+_size)
        println("Nr of distinct relations: "+WordDomain.size)

        println("Preparing sampler")
        val sampler = SparseLDAInferencer(ZDomain, WordDomain, documents, alphas.value, beta1, model)

        println("LDA clusering")
        for (i <- 1 to 100) {
            for (doc <- documents) sampler.process(doc.zs)
            if (i % 5 == 0) {
                phis.foreach(phi => phi.value.masses.foreachElement { case (idx,mass) => phi.value.masses += (idx, -mass) })
                sampler.export(phis)
                if (fitDirichlet) {
                    sampler.exportThetas(documents)
                    MaximizeDirichletByMomentMatching(alphas, model)
                    sampler.resetSmoothing(alphas.value, beta1)
                }
            }
        }
        println("Gathering and writing results")
        val topicProbs = new DenseProportions1(numTopics)
        documents.foreach(_.zs.discreteValues.foreach(z =>
            topicProbs.masses += (z.intValue, 1)
        ))

        val topicsGivenRelation = Array.fill[DenseProportions1](WordDomain.size)(new DenseProportions1(numTopics))
        (0 until numTopics).foreach(topic => {
            var topRels = List[(String,Double)]()
            val pw = new PrintWriter(new FileWriter(new File(s"/data/clusters/topic$topic-%1.3f".format(topicProbs(topic)))))
            phis(topic).value.foreachElement {
                case (relationIdx,p_R_T) =>
                    topicsGivenRelation(relationIdx).masses += (topic,p_R_T* topicProbs(topic))
                    topRels ::= ((WordDomain.category(relationIdx),p_R_T))
            }
            var totalProb = 0.0

            var totalMass =  phis(topic).value.masses.foldLeft(0.0)(_ + _)

            topRels.sortBy(-_._2).takeWhile(rel => {
                totalProb += rel._2
                totalProb <= 0.8
            }).foreach(rel =>
                pw.println("%1.4f\t%s".format(rel._2,rel._1)))
            pw.close()
        })

        System.exit(0)
    }
}
