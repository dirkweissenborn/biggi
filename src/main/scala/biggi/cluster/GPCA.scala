package biggi.cluster

import org.apache.commons.logging.LogFactory
import java.io._
import biggi.util.BiggiUtils._
import com.thinkaurelius.titan.core.{TitanEdge, TitanFactory}
import com.tinkerpop.blueprints.Direction
import biggi.model.deppath.DependencyPath
import scala.collection.JavaConversions._
import cc.factorie.la._
import scala.util.Random
import scala.collection.mutable.ListBuffer
import cc.factorie.model._
import cc.factorie.variable._
import cc.factorie.infer._
import cc.factorie.maths._
import cc.factorie.optimize._
import cc.factorie.util.{StringMapCubbie, BinarySerializer}
import cc.factorie._
import scala.collection.mutable
import cc.factorie.variable.HashMapAssignment
import cc.factorie.la.DenseTensor1
import cc.factorie.model.Parameters
import cc.factorie.optimize.Example
import cc.factorie.variable.Var
import cc.factorie.la.Tensor1
import cc.factorie.variable.DiscreteDomain
import cc.factorie.model.Model
import cc.factorie.model.Factor

/**
 * Created by dirkw on 1/23/14.
 */
object GPCA {

    private final val LOG = LogFactory.getLog(getClass)

    private var numDims = 100

    class GPCAModel(weightsTensor:Tensor1) extends Model with Parameters {
        val weights = new DotFamilyWithStatistics1[VectorVar] {
            override val weights: Weights1 = Weights(new DenseTensor1(weightsTensor.length))
            weights.set(weightsTensor)
        }

        def factors(variables: Iterable[Var]): Iterable[Factor] = {
            variables.map {
                case v:VectorVar => weights.Factor(v)
                case _ => null
            }
        }
    }

    object GPCAInferer extends Infer[Iterable[VectorVar],GPCAModel] {
        def infer(variables: Iterable[VectorVar], model: GPCAModel, marginalizing: Summary): Summary = {
            val assignment = new HashMapAssignment(variables)
            model.factors(variables).foreach{
                case factor:model.weights.FactorType => assignment.update[VectorVar](factor._1, factor._1.value.*(sigmoid(factor.currentScore)).asInstanceOf[factor._1.Value])
            }
            val fs = model.factors(variables).toSeq
            new MAPSummary(assignment, fs) {
                override def logZ = 0.0//fs.map(f => math.log(sigmoid(f.currentScore))).sum
            }
        }
    }

    def main(args:Array[String]) {
        val graphsDir = new File(args(0))
        val outDir = new File(args(1))
        if(args.size > 2)
            numDims = args(2).toInt

        var relationVectors = mutable.HashMap[String,(ListBuffer[String],GPCAModel)]()
        var pairVectors = mutable.HashMap[String, (ListBuffer[String],GPCAModel)]()

        var usingOldTensors = false
        if(args.size > 3) {
            //StartingVectors
            relationVectors = deserialize(new FileInputStream(new File(args(3)))).foldLeft(relationVectors){ case (acc,(key,t)) =>  acc += key -> (ListBuffer[String](),new GPCAModel(t)); acc }
            pairVectors = deserialize(new FileInputStream(new File(args(4)))).foldLeft(pairVectors){ case (acc,(key,t)) =>  acc += key -> (ListBuffer[String](),new GPCAModel(t)); acc }
            usingOldTensors = true
        }

        outDir.mkdirs()

        var ctr = 0


        val graphs = graphsDir.listFiles().map(graphDir => {
            val conf = getGraphConfiguration(graphDir)
            conf.addProperty("storage.transactions","false")

            val graph = TitanFactory.open(conf)
            val cuiId = getCuiToID(graphDir)
            (graph, cuiId)
        })

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

            map.foreach {  case ((cui1,cui2),paths) =>
                val pair = cui1 + "-" + cui2
                if(!usingOldTensors || !pairVectors.contains(pair)) {
                    val randomTensor = Tensor.tabulate(numDims)(_ => Random.nextGaussian())
                    pairVectors += pair -> (ListBuffer(paths.map(_._1):_*),new GPCAModel(randomTensor))
                }
                else pairVectors(pair)._1 ++= paths.map(_._1)

                paths.foreach(path => {
                    if(!relationVectors.contains(path._1)) {
                        val randomTensor = Tensor.tabulate(numDims)(_ => Random.nextGaussian())
                        relationVectors += path._1 -> (ListBuffer(pair),new GPCAModel(randomTensor))
                    }
                    else
                        relationVectors(path._1)._1 += pair
                })
            }

            ctr += 1
            if(ctr % 10000 == 0)
                LOG.info(s"$ctr vertices processed")
        })
        graphs.foreach(_._1.shutdown())


        def newExample(tensor: Tensor1, model: GPCA.GPCAModel): LikelihoodExample[Iterable[VectorVar], GPCA.GPCAModel] = {
            new LikelihoodExample(
                Iterable(new VectorVariable(tensor) {
                    def domain: VectorDomain = new VectorDomain {
                        def dimensionDomain: DiscreteDomain = new DiscreteDomain(numDims)
                    }
                }), model, GPCAInferer)
        }

        implicit val random = Random

        def calcAverageLogLikelihood: Double = {
            var total = 0
            val logLikelihood = pairVectors.values.foldLeft(0.0) {
                case (acc, (examples, model)) =>
                    acc + examples.map(ex => {
                        total += 1
                        math.log(sigmoid(relationVectors(ex)._2.weights.weights.value dot model.weights.weights.value))
                    }).sum
            }
            logLikelihood
        }
        var oldLL = Double.MinValue
        var ll = calcAverageLogLikelihood
        println(s"Nr. of pairs: ${pairVectors.size}")
        println(s"Nr. of relations: ${relationVectors.size}")
        println(s"Before training: log L = $ll")
        var counter = 0
        (1 to 10).foreach(i => if(ll - oldLL > 0.01 ){
            println(s"Starting $i-th iteration")
            counter = 0
            pairVectors.values.par.foreach{
                case (examples,model) =>
                    train(model.parameters, examples.map(rel => newExample(relationVectors(rel)._2.weights.weights.value, model)), maxIterations = 5)
                    counter += 1
                    if(counter % 10000 == 0)
                        println(s"$counter pairs processed")
            }
            println(s"${i-1}.5-th iteration: log L = $calcAverageLogLikelihood")
            counter = 0
            relationVectors.values.par.foreach{
                case (examples,model) =>
                    train(model.parameters, examples.map(pair => newExample(pairVectors(pair)._2.weights.weights.value, model)), maxIterations = 5)
                    counter += 1
                    if(counter % 1000 == 0)
                        println(s"$counter relations processed")
            }

            oldLL = ll
            ll = calcAverageLogLikelihood
            println(s"$i-th iteration: log L = $calcAverageLogLikelihood")
        })

        serialize(new FileOutputStream(new File(outDir,"pair_vectors.bin")),pairVectors.foldLeft(mutable.HashMap[String,DenseTensor1]())((acc,e) => acc += e._1 -> e._2._2.weights.weights.value.asInstanceOf[DenseTensor1]))
        serialize(new FileOutputStream(new File(outDir,"relation_vectors.bin")),relationVectors.foldLeft(mutable.HashMap[String,DenseTensor1]())((acc,e) => acc += e._1 -> e._2._2.weights.weights.value.asInstanceOf[DenseTensor1]))

        println("done")

        System.exit(0)
    }

    def serialize(stream: OutputStream, vectors:mutable.HashMap[String,DenseTensor1]) {
        val dstream = new DataOutputStream(new BufferedOutputStream(stream))
        BinarySerializer.serialize(new StringMapCubbie[DenseTensor1](vectors), dstream)
        dstream.close()
    }

    def deserialize(stream: InputStream) = {
        val dstream = new DataInputStream(new BufferedInputStream(stream))
        val vectors = mutable.HashMap[String,DenseTensor1]()
        BinarySerializer.deserialize(new StringMapCubbie[DenseTensor1](vectors),dstream)
        dstream.close()
        vectors
    }

    //No output
    class MyParallelBatchTrainer(val weightsSet: WeightsSet, val optimizer: GradientOptimizer, val nThreads: Int = Runtime.getRuntime.availableProcessors())
        extends Trainer {
        val gradientAccumulator = new SynchronizedWeightsMapAccumulator(weightsSet.blankDenseMap)
        val valueAccumulator = new SynchronizedDoubleAccumulator
        def processExamples(examples: Iterable[Example]): Unit = {
            if (isConverged) {
                return
            }
            gradientAccumulator.l.tensorSet.zero()
            valueAccumulator.l.accumulate(-valueAccumulator.l.value)
            util.Threading.parForeach(examples.toSeq, nThreads)(_.accumulateValueAndGradient(valueAccumulator, gradientAccumulator))
            optimizer.step(weightsSet, gradientAccumulator.tensorSet, valueAccumulator.l.value)
        }
        def isConverged = optimizer.isConverged
    }

    def train(parameters: WeightsSet, examples: Seq[Example], maxIterations: Int, optimizer: GradientOptimizer = new AdaGrad(), nThreads: Int = Runtime.getRuntime.availableProcessors())(implicit random: scala.util.Random) {
        val trainer = new MyParallelBatchTrainer(parameters, optimizer=optimizer, nThreads=nThreads)
        try {
            var its = 0
            while (!trainer.isConverged && its < maxIterations) {
                trainer.processExamples(examples)
                //optimizer match { case o: ParameterAveraging => o.setWeightsToAverage(parameters); case _ => }
                //optimizer match { case o: ParameterAveraging => o.unSetWeightsToAverage(parameters); case _ => }
                its += 1
            }
            //LOG.info(TrainerHelpers.getBatchTrainerStatus(trainer.gradientAccumulator.l.tensorSet.oneNorm, trainer.valueAccumulator.l.value, System.currentTimeMillis()))
        } finally {
            trainer match { case t: ParallelOnlineTrainer => t.removeLocks(); case _ => }
            optimizer.finalizeWeights(parameters)
        }
    }
}
