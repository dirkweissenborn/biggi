package biggi.classify

import cc.factorie.variable._
import biggi.model.factorie.HMMModel
import cc.factorie.la.{Tensor1, Tensor, DenseTensor1}
import cc.factorie.app.classify.backend.{MulticlassClassifier}
import scala._

/**
 * @author dirk
 *          Date: 12/16/13
 *          Time: 4:22 PM
 */

object PathLabelDomain extends CategoricalDomain[String] {
    index("true")
    index("false")
    freeze()
}

class PathLabelVariable(isTrue:Boolean, val features: PathFeatureVariable) extends LabeledCategoricalVariable[String](if(isTrue) "true" else "false") {
    def domain = PathLabelDomain
}

class PairLabelVariable(isTrue:Boolean, val paths: Seq[PathLabelVariable]) extends LabeledCategoricalVariable[String](isTrue.toString) {
    def domain = PathLabelDomain
}

class PathFeatureVariable(features:Seq[FeatureVectorVariable[String]], val path:Seq[String]) extends SeqVariable[FeatureVectorVariable[String]](features)

trait PathClassifier extends MulticlassClassifier[PathFeatureVariable]

class HMMClassifier(val posModel:HMMModel, val negModel:HMMModel) extends PathClassifier {
    def predict(input: PathFeatureVariable) = {
        val posFeaturesSeq = posModel.otherToModelFeatures(input.value)
        val negFeaturesSeq = negModel.otherToModelFeatures(input.value)

        val posResult = posModel.inferFast(posFeaturesSeq)
        val negResult = negModel.inferFast(negFeaturesSeq)

        var result = new DenseTensor1(2)
        result += (PathLabelDomain.index("true"), posResult.logZ)
        result += (PathLabelDomain.index("false"), negResult.logZ)
        if(result.forallElements{ case (_,value) => value == Double.NegativeInfinity })
            result = Tensor.tabulate(2)(_ => 0.0)

        result
    }
}

class PathClassifierFromModel(val maxLengthOfPath:Int, model:MulticlassClassifier[Tensor1]) extends PathClassifier {
    def predict(input: PathFeatureVariable) = {
        model.predict(PathClassifierFromModel.seqToFeature(input, maxLengthOfPath))
    }
}

object PathClassifierFromModel {
    def seqToFeature(input: PathFeatureVariable, maxPathLength:Int): DenseTensor1 = {
        val inpArray = input.value.toArray
        val dimOfFeatureVectors = inpArray(0).value.length
        Tensor.tabulate(maxPathLength * dimOfFeatureVectors)(i => {
            val feature = i / dimOfFeatureVectors
            if (feature >= inpArray.size)
                0.0
            else
                inpArray(feature).value(i % dimOfFeatureVectors)
        })
    }
}

class PairClassifier(pathClassifier:PathClassifier, scoreCombiner: Seq[Tensor1] => Tensor1, evaluatePredictionDetails:Seq[(PathLabelVariable,Tensor1)] => Unit = null) extends MulticlassClassifier[Seq[PathLabelVariable]] {
    def predict(input: Seq[PathLabelVariable]) = {
        val output = input.map(i => (i,pathClassifier.predict(i.features)))
        if(evaluatePredictionDetails != null)
            evaluatePredictionDetails(output)
        if(output.isEmpty)
            Tensor.tabulate(2)(_ => 0.0)
        else
            scoreCombiner(output.map(_._2))
    }
}


