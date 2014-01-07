package biggi.cluster

import java.io._
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
import biggi.model.deppath.DependencyPath
import cc.factorie.util.{BinarySerializer}
import cc.factorie._
import org.apache.commons.logging.LogFactory

/**
 * @author dirk
 *          Date: 12/5/13
 *          Time: 2:06 PM
 */
object ClusterEdges {
    private final val LOG = LogFactory.getLog(getClass)

    implicit val random = new Random(0)
    implicit val model = DirectedModel()

    val numTopics = 100
    val beta1 = 0.1
    val alpha1 = 0.1
    val fitDirichlet = false

    var avgNrOfConnections = 0.0

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

    var phis = Mixture(numTopics)(new ProportionsVariable(new GrowableDenseProportions1(WordDomain){ override def update(idx:Int,value:Double) = this.masses += (idx,value) }) ~ Dirichlet(beta))
    var topicProbs = new DenseProportions1(numTopics){ override def update(idx:Int,value:Double) = this.masses += (idx,value) }

    def main(args:Array[String]) {
        val graphsDir = new File(args(0))
        val outDir = new File(args(1))
        outDir.mkdirs()

        var ctr = 0

        val documents = new ArrayBuffer[Document]

        val graphs = graphsDir.listFiles().map(graphDir => {
            val conf = getGraphConfiguration(graphDir)
            conf.addProperty("storage.transactions","false")

            val graph = TitanFactory.open(conf)
            val cuiId = getCuiToID(graphDir)
            (graph, cuiId)
        })

        def getSemTypes(cui:String):Set[String] = {
            graphs.foldLeft(Set[String]()){ case (acc,(g,cuiId)) =>
                if(cuiId.containsKey(cui) && acc.isEmpty) {
                    val id = cuiId(cui)
                    val ts = g.getVertex(id).getProperty[String](TYPE)
                    if(ts != null)
                        ts.split(",").toSet
                    else
                        acc
                }
                else acc
            }
        }

        val cuis = graphs.map(_._2.keySet()).reduce(_.intersect(_))

        LOG.info("Collecting edges from graph...")
        cuis.foreach(cui => {
            //(fromCui,toCui) -> depPaths
            var map = Map[(String,String),List[(DependencyPath,String,String)]]()
            graphs.foreach{ case (graph,cuiId) =>
                val id = cuiId(cui)
                graph.getVertex(id).getEdges(Direction.BOTH).foreach {
                    case edge:TitanEdge =>
                        val fromCui = edge.getVertex(Direction.OUT).getProperty[String](UI)
                        val toCui = edge.getVertex(Direction.IN).getProperty[String](UI)

                        val otherCui = if(fromCui == cui) toCui else fromCui
                        if(cui < otherCui) {
                            val oldDepPaths = map.getOrElse((cui,otherCui), List[(DependencyPath,String,String)]())
                            map += (cui,otherCui) -> (oldDepPaths ++  {
                                val label = edge.getProperty[String](LABEL)
                                val depPath = DependencyPath.fromString(label)

                                if(depPath != null) {
                                    val nr = edge.getProperty[String](SOURCE).count(_ == ',') +1
                                    for(i <- 0 until nr) yield (depPath,fromCui,toCui)
                                }
                                else List[(DependencyPath,String,String)]()
                            })
                        }
                    }
            }
            map.foreach {  case ((cui1,cui2),paths) =>
                if(!paths.isEmpty && paths.tail.exists(!_.equals(paths.head))){
                    val theta = ProportionsVariable.sortedSparseCounts(numTopics) ~ Dirichlet(alphas)

                    //val ts = Map(cui1 -> getSemTypes(cui1), cui2 -> getSemTypes(cui2))

                    //val zs = new Zs(paths.length*ts.map(_._2.size).product) :~ PlatedDiscrete(theta)
                    val zs = new Zs(paths.length) :~ PlatedDiscrete(theta)

                    documents += new Document(cui1 +"-"+ cui2, theta, zs,
                        paths.map(_._1.toShortString) ) ~ PlatedCategoricalMixture(phis, zs)
                    //paths.flatMap(p => ts(p._2).flatMap(t1 => ts(p._3).map(t2 => t1+"-"+p._1.toShortString+"-"+t2)))
                }
            }
            ctr += 1
            if(ctr % 10000 == 0)
                LOG.info(s"$ctr vertices processed")
        })
        graphs.foreach(_._1.shutdown())
        val _size = documents.size
        LOG.info("Avg. nr of connections: "+(avgNrOfConnections / _size))
        LOG.info("Nr of concept pairs: "+_size)
        LOG.info("Nr of distinct relations: "+WordDomain.size)

        LOG.info("Preparing sampler")
        val sampler = SparseLDAInferencer(ZDomain, WordDomain, documents, alphas.value, beta1, model)

        LOG.info("LDA clusering")
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
        LOG.info("Gathering and writing results")
        topicProbs = new DenseProportions1(numTopics)
        documents.foreach(_.zs.discreteValues.foreach(z =>
            topicProbs.masses += (z.intValue, 1)
        ))

        val topicsGivenRelation = Array.fill[DenseProportions1](WordDomain.size)(new DenseProportions1(numTopics))
        (0 until numTopics).foreach(topic => {
            var topRels = List[(String,Double)]()
            val pw = new PrintWriter(new FileWriter(new File(outDir,s"topic$topic-%1.3f".format(topicProbs(topic)))))
            phis(topic).value.foreachElement {
                case (relationIdx,p_R_T) =>
                    topicsGivenRelation(relationIdx).masses += (topic,p_R_T* topicProbs(topic))
                    topRels ::= ((WordDomain.category(relationIdx),p_R_T))
            }
            var totalProb = 0.0

            topRels.sortBy(-_._2).takeWhile(rel => {
                totalProb += rel._2
                totalProb <= 0.8
            }).foreach(rel =>
                pw.println("%1.4f\t%s".format(rel._2,rel._1)))
            pw.close()
        })

        serialize(new FileOutputStream(new File(outDir,"clusteredDepPaths.bin")))

        System.exit(0)
    }

    def getTopicGivenRel = {
        val topicsGivenRelation = Array.fill[DenseProportions1](WordDomain.size)(new DenseProportions1(numTopics))
        (0 until numTopics).foreach(topic => {
            phis(topic).value.foreachElement {
                case (relationIdx,p_R_T) =>
                    topicsGivenRelation(relationIdx).masses += (topic,p_R_T* topicProbs(topic))
            }
        })
        topicsGivenRelation
    }

    def serialize(stream: OutputStream) {
        val dstream = new DataOutputStream(new BufferedOutputStream(stream))
        BinarySerializer.serialize(WordDomain, dstream)
        BinarySerializer.serialize(phis.value, dstream)
        BinarySerializer.serialize(topicProbs, dstream)
        dstream.close()
    }

    def deserialize(stream: InputStream) {
        val dstream = new DataInputStream(new BufferedInputStream(stream))
        BinarySerializer.deserialize(WordDomain, dstream)
        BinarySerializer.deserialize(phis.value, dstream)
        BinarySerializer.deserialize(topicProbs, dstream)
        dstream.close()
    }
}
