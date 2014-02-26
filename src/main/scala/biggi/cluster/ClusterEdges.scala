package biggi.cluster

import java.io._
import biggi.util.BiggiUtils._
import com.thinkaurelius.titan.core.{TitanEdge, TitanFactory}
import com.tinkerpop.blueprints.Direction
import scala.collection.JavaConversions._
import scala.Predef._
import scala.util.Random
import cc.factorie.app.topics.lda.SparseLDAInferencer
import cc.factorie.directed._
import cc.factorie.variable._
import scala.collection.mutable.ArrayBuffer
import biggi.model.deppath.DependencyPath
import cc.factorie.util.{StringMapCubbie, BinarySerializer}
import cc.factorie._
import org.apache.commons.logging.LogFactory
import cc.factorie.la.SparseIndexedTensor1
import scala.collection.mutable

/**
 * @author dirk
 *          Date: 12/5/13
 *          Time: 2:06 PM
 */
object ClusterEdges {
    private final val LOG = LogFactory.getLog(getClass)

    implicit val random = new Random(0)
    implicit val model = DirectedModel()

    var numTopics = 100
    val beta1 = 0.0001
    val alpha1 = 0.0001
    val fitDirichlet = true

    var avgNrOfConnections = 0.0

    lazy val ZDomain = new DiscreteDomain(numTopics)
    val ZSeqDomain = new DiscreteSeqDomain { def elementDomain = ZDomain }
    class Zs(len:Int) extends DiscreteSeqVariable(Array.fill(len)(Random.nextInt(numTopics))) {
        def domain = ZSeqDomain
    }
    object WordSeqDomain extends CategoricalSeqDomain[String]
    val WordDomain = WordSeqDomain.elementDomain
    WordDomain.gatherCounts = true
    class Document(name:String, myTheta:ProportionsVariable, myZs:Zs, words:Seq[String]) extends cc.factorie.app.topics.lda.Document(WordSeqDomain, name, words) {
        words.foreach(w => WordDomain.index(w))
        this.theta = myTheta
        this.zs = myZs
    }
    lazy val beta = MassesVariable.growableUniform(WordDomain, beta1)
    lazy val alphas = MassesVariable.dense(numTopics, alpha1)

    lazy val phis = Mixture(numTopics)(new ProportionsVariable(new GrowableDenseProportions1(WordDomain){ override def update(idx:Int,value:Double) = this.masses += (idx,value) }) ~ Dirichlet(beta))
    lazy val topicProbs = new DenseProportions1(numTopics){ override def update(idx:Int,value:Double) = this.masses += (idx,value) }

    def main(args:Array[String]) {
        val graphsDir = new File(args(0))
        val outDir = new File(args(1))

        if(args.size > 2)
            numTopics = args(2).toInt

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

        val cuis = graphs.map(_._2.keySet()).reduce(_ ++ _)

        LOG.info("Collecting edges from graph...")
        cuis.foreach(cui => {
            //conceptPair -> List(depPath, fromCUI, toCUI)
            var map = Map[(String,String),List[(String,String,String)]]()
            graphs.foreach{ case (graph,cuiId) =>
                val id = cuiId.get(cui)
                if(id != null)
                    graph.getVertex(id).getEdges(Direction.BOTH).foreach {
                        case edge:TitanEdge =>
                            val fromCui = edge.getVertex(Direction.OUT).getProperty[String](UI)
                            val toCui = edge.getVertex(Direction.IN).getProperty[String](UI)

                            val otherCui = if(fromCui == cui) toCui else fromCui
                            if(cui < otherCui) {
                                val oldDepPaths = map.getOrElse((cui,otherCui), List[(String,String,String)]())
                                map += (cui,otherCui) -> (oldDepPaths ++  {
                                    val label = edge.getProperty[String](LABEL)
                                    val depPath = DependencyPath.removeAttributes(label,withNeg = true)

                                    if(depPath != null) {
                                        val nr = edge.getProperty[String](SOURCE).count(_ == ',') +1
                                        for(i <- 0 until nr) yield (depPath,fromCui,toCui)
                                    }
                                    else List[(String,String,String)]()
                                })
                            }
                        }
            }
            map.foreach {  case ((cui1,cui2),relations) =>
                if(!relations.isEmpty){
                    val theta = ProportionsVariable.sortedSparseCounts(numTopics) ~ Dirichlet(alphas)
                    //val semtypes2 = getSemTypes(cui2).toList
                    //val semTypes = getSemTypes(cui1).toList.flatMap(st1 => semtypes2.map(st2 => if(st1 < st2) "semtypes:" + st1+"-"+st2 else  "semtypes:" + st2+"-"+st1))

                    var features = relations.map(_._1)

                    /*val keywords = features.foldLeft(mutable.Map[String,Int]())((acc, depPath) => {
                        val lemmas = """(?<= )[a-zA-Z]+(?=(:(?!prep|attr|agent))| )""".r.findAllIn(depPath).matchData.map(_.toString).toList
                        lemmas.foreach(lemma => if(acc.contains(lemma)) acc(lemma) += 1 else acc += lemma -> 1)
                        acc
                    }).filter(_._2 > 1).toSeq.sortBy(-_._2).take(5)*/

                    //keywords.foreach(el => (0 until el._2).foreach(_ => features ::= "keyword:"+el._1))

                    avgNrOfConnections += relations.size

                    //(0 until relations.size).foreach(_ => features ++= semTypes)

                    val zs = new Zs(features.length) :~ PlatedDiscrete(theta)


                    documents += new Document(cui1 +"-"+ cui2, theta, zs, features) ~ PlatedCategoricalMixture(phis, zs)
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
                println(i +" of 100 iterations training done")
                phis.foreach(phi => phi.value.masses.foreachElement { case (idx,mass) => phi.value.masses += (idx, -mass) })
                sampler.export(phis)
                if (fitDirichlet) {
                    sampler.exportThetas(documents)
                    MaximizeDirichletByMomentMatching(alphas, model)
                    sampler.resetSmoothing(alphas.value, beta1)
                }
            }
        }

        if(fitDirichlet) {
            sampler.exportThetas(documents)
            serializeThetas(new FileOutputStream(new File(outDir,"thetas.bin")),documents)
        }

        LOG.info("Gathering and writing results")
        documents.foreach(_.zs.discreteValues.foreach(z =>
            topicProbs.masses += (z.intValue, 1)
        ))

        serialize(new FileOutputStream(new File(outDir,"clusteredDepPaths.bin")))

        (0 until numTopics).foreach(topic => {
            val pw = new PrintWriter(new FileWriter(new File(outDir,"topic %d-%1.3f".format(topic, topicProbs(topic)))))
            val paths = phis(topic).value.masses.toArray.zipWithIndex.sortBy(-_._1).take(100)
            val total = phis(topic).value.massTotal
            paths.foreach(el => pw.println(WordDomain.category(el._2)+"\t"+(el._1/total)))
            pw.close()
        })

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

    def serializeThetas(stream: OutputStream, documents:ArrayBuffer[Document]) {
        val dstream = new DataOutputStream(new BufferedOutputStream(stream))
        BinarySerializer.serialize(mutable.HashMap[String,Int]("size"->documents.size,"dim" -> documents.head.theta.value.dimensions(0)),dstream)
        BinarySerializer.serialize(new StringMapCubbie[Tensor1](documents.foldLeft(scala.collection.mutable.Map[String,Tensor1]())((acc,d) => acc += d.name -> d.theta.value.asInstanceOf[Tensor1])), dstream)
        dstream.close()
    }

    def deserializeThetas(stream: InputStream) = {
        val dstream = new DataInputStream(new BufferedInputStream(stream))
        val paras = mutable.HashMap[String,Int]()
        BinarySerializer.deserialize(paras,dstream)
        val documents = scala.collection.mutable.Map[String,SparseIndexedTensor1]()
        (0 until paras("size")).foreach(d => documents += d.toString -> new SparseIndexedTensor1(paras("dim")))
        BinarySerializer.deserialize(new StringMapCubbie[SparseIndexedTensor1](documents), dstream)
        dstream.close()
        documents
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
