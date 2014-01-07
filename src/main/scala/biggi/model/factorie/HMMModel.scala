package biggi.model.factorie

import cc.factorie._
import scala.collection.mutable.{ListBuffer,ArrayBuffer}
import java.io._
import cc.factorie.util.{DoubleAccumulator, BinarySerializer}
import cc.factorie.variable._
import scala.reflect.ClassTag
import cc.factorie.model._
import cc.factorie.app.chain.Observation
import cc.factorie.infer.{InferByBPChain, Infer}
import cc.factorie.optimize.{Trainer, GradientOptimizer, Example}
import cc.factorie.la.{GrowableSparseTensor1, Tensor1, Tensor2, WeightsMapAccumulator}
import cc.factorie.Tensor2
import cc.factorie.Tensor1
import scala.collection.mutable
import scala._

class HMMModel(val nrHiddenLabels:Int, val featuresDomain:CategoricalVectorDomain[String])
    extends Model with Parameters {
    self =>

    implicit val rand = new scala.util.Random

    object LabelDomain extends DiscreteDomain(nrHiddenLabels)

    protected class FeatureChain extends Chain[FeatureChain, Features]

    class Features(features: Seq[String]) extends FeatureVectorVariable[String] with Observation[Features] with ChainLink[Features, FeatureChain] {
        set(new GrowableSparseTensor1(featuresDomain.dimensionDomain))(null)

        this ++= features

        val string = "N/A"
        override def domain = featuresDomain
        override val skipNonCategories = true

        val label = new Label(this)
    }

    protected class Label(val features: Features) extends DiscreteVariable {
        override def domain = LabelDomain
    }

    protected val labelToFeatures: Label => Features = _.features
    protected val tokenToLabel: Features => Label = _.label
    
    val initial = new DotFamilyWithStatistics1[Label] {
        factorName = "Label"
        val weights = Weights(new la.DenseTensor1(LabelDomain.size))
    }
    val obs = new DotFamilyWithStatistics2[Label, Features] {
        factorName = "Label,Observation"
        val weights = Weights(new la.DenseTensor2(LabelDomain.dimensionSize, featuresDomain.dimensionSize))
    }
    val markov = new DotFamilyWithStatistics2[Label, Label] {
        factorName = "Label,Label"
        val weights = Weights(new la.DenseTensor2(LabelDomain.size, LabelDomain.size))
    }

    def serialize(stream: OutputStream) {
        val dstream = new DataOutputStream(new BufferedOutputStream(stream))
        BinarySerializer.serialize(featuresDomain, dstream)
        BinarySerializer.serialize(LabelDomain, dstream)
        BinarySerializer.serialize(this, dstream)
        dstream.close()
    }

    def deserialize(stream: InputStream) {
        val dstream = new DataInputStream(new BufferedInputStream(stream))
        BinarySerializer.deserialize(featuresDomain, dstream)
        BinarySerializer.deserialize(LabelDomain, dstream)
        BinarySerializer.deserialize(this, dstream)
        dstream.close()
    }

    override def factors(v: Var) = v match {
        case label: Label =>
            val result = new ArrayBuffer[Factor](4)
            if(!labelToFeatures(label).hasPrev)
                result += initial.Factor(label)
            result += obs.Factor(label, labelToFeatures(label))
            val features = labelToFeatures(label)
            if (features.hasPrev) {
                result += markov.Factor(tokenToLabel(features.prev), label)
            }
            if (features.hasNext) {
                result += markov.Factor(label, tokenToLabel(features.next))
            }
            result
    }

    override def factors(variables: Iterable[Var]): Iterable[Factor] = {
        val result = new ListBuffer[Factor]
        variables match {
            case labels: Iterable[Label] =>
                var prevLabel: Label = null.asInstanceOf[Label]
                for (label <- labels) {
                    if(!labelToFeatures(label).hasPrev)
                        result += initial.Factor(label)
                    result += obs.Factor(label, labelToFeatures(label))
                    if (prevLabel ne null) {
                        result += markov.Factor(prevLabel, label)
                    }
                    prevLabel = label
                }
        }
        result
    }

    def trainBaumWelch(examples:Seq[HMMExample], maxNrOfIterations:Int = 100) {
        Trainer.batchTrain(parameters,
            examples,
            optimizer = new HMMOptimizer(maxIts = maxNrOfIterations),
            useParallelTrainer = true,
            maxIterations = maxNrOfIterations)
    }

    def otherToModelFeatures(fvs: Seq[FeatureVectorVariable[String]]): Seq[Features] = {
        val result = fvs.map(fv => {
            val features = new Features(Seq.empty[String])
            if(fv.domain == featuresDomain)
                features.set(fv.value)(null)
            else
                fv.value.foreachActiveElement{ case (idx,value) => features +=(fv.domain.dimensionDomain.category(idx), value) }
            features
        })
        new FeatureChain ++= result

        result
    }

    def inferFast(observations:Seq[Features]) = {
        InferByBPChain.infer(observations.map(_.label),this)
    }

    class HMMExample(observationVectors: Seq[self.Features], fac:Double = 1.0) extends Example {
        val labels = observationVectors.map(_.label)
        new FeatureChain ++= observationVectors

        def accumulateValueAndGradient(value: DoubleAccumulator, gradient: WeightsMapAccumulator): Unit = {
            val summary = InferByBPChain.infer(labels, self)
            if (value != null)
                value.accumulate(summary.logZ)
            val factors = summary.factorMarginals
            if (gradient != null) {
                for (factorMarginal <- factors; factorU <- factorMarginal.factor) {
                    factorU match {
                        case factor:DotFamily#Factor =>
                            val tensorStatistics = summary.marginal(factor).tensorStatistics
                            gradient.accumulate(factor.family.weights, tensorStatistics, fac)
                        case _ =>
                    }
                }
            }
        }
    }

    class HMMOptimizer(var minChange:Double = 0.1, var maxIts:Int = 100) extends GradientOptimizer {
        private var its = 0
        private var oldVal = Double.MaxValue
        private var change = Double.MaxValue

        def step(weights: WeightsSet, gradient: WeightsMap, value: Double) = {
            weights.keys.foreach(weight => {
                weight.value.dimensions match {
                    case Array(dim1,dim2) =>
                        val gWeight = gradient(weight).asInstanceOf[Tensor2]
                        (0 until dim1).foreach(i => {
                            var sum = 0.0
                            (0 until dim2).foreach(j => sum += gWeight(i,j))
                            if(sum > 0)
                                (0 until dim2).foreach(j => weight.value.asInstanceOf[Tensor2].update(i,j,math.log(gWeight(i,j)/sum)))
                            else
                                (0 until dim2).foreach(j => weight.value.asInstanceOf[Tensor2].update(i,j,math.log(1.0/dim2)))
                        })
                    case Array(dim1) =>
                        val gWeight = gradient(weight).asInstanceOf[Tensor1].normalized
                        weight.value.foreachElement{ case (idx,value) =>
                            val newVal = math.log(gWeight(idx))
                            weight.value.update(idx,newVal)
                        }
                    case _ =>
                }
            })
            its += 1
            change = math.abs(oldVal-value)
            oldVal = value
        }

        def isConverged = its >= maxIts || change < minChange

        def reset() = {
            its = 0
            change = Double.MaxValue
            oldVal = Double.MaxValue
        }

        def initializeWeights(weights: WeightsSet) =  {
            val rand = new scala.util.Random()
            weights.keys.foreach(weights => {
                weights.value.foreachElement {
                    case (idx, _) => weights.value.update(idx, rand.nextDouble())
                }
            })
        }

        def finalizeWeights(weights: WeightsSet) = { }

    }

}
