package biggi.classify

import cc.factorie.variable._
import cc.factorie.app.classify._
import biggi.model.factorie.HMMModel
import biggi.model.deppath.{DependencyPath, GraphPathStore}
import scala.collection.JavaConversions._
import cc.factorie.la.{DenseTensor1, Tensor, SparseIndexedTensor1, Tensor1}
import java.io.{FileWriter, PrintWriter, FileInputStream, File}
import biggi.cluster.{GPCA, ClusterEdges}
import scala.collection.mutable
import scala.util.Random
import scala.Array
import cc.factorie.app.classify.backend._
import scopt.OptionParser
import scala.Some
import scopt.OptionParser
import scala.Some
import cc.factorie.app.classify.LabeledClassification

/**
 * @author dirk
 *          Date: 12/16/13
 *          Time: 2:18 PM
 */
object TrainPathClassifier {
    private implicit val rand = Random

    trait TrainingPolicy {
        def train(trainingExamples:Seq[(PathLabelVariable,Double)]):PathClassifier
    }

    class NoiseReductionTraining(training:TrainingPolicy) extends TrainingPolicy {
        def train(trainingExamples: Seq[(PathLabelVariable,Double)]) = {
            //var diff = Set[PathLabelVariable]()
            var classifier = training.train(trainingExamples)
            (0 until 2).foreach(i => {
                classifier = training.train(trainingExamples.filterNot(e => e._1.categoryValue == "true" && classifier.classification(e._1.features).proportions(trueDim) < 0.6))
            })
            classifier
        }
    }

    class HMMTraining(val numLabels:Int) extends TrainingPolicy {
        def train(trainingExamples: Seq[(PathLabelVariable,Double)]) = {
            val featuresDomain = trainingExamples.head._1.features.head.domain

            val posModel = new HMMModel(numLabels,featuresDomain)
            val negModel = new HMMModel(numLabels,featuresDomain)

            trainingExamples.groupBy(_._1.categoryValue).foreach {
                case ("true",posExamples) =>
                    posModel.trainBaumWelch(posExamples.map{ case (l,weight) => new posModel.HMMExample(posModel.otherToModelFeatures(l.features.value), weight) }, 10)
                case ("false",negExamples) =>
                    negModel.trainBaumWelch(negExamples.map{ case (l,weight) => new negModel.HMMExample(negModel.otherToModelFeatures(l.features.value), weight) }, 10)
            }

            new HMMClassifier(posModel,negModel)
        }
    }
    class ModelTraining[M <: MulticlassClassifier[Tensor1]](val maxPathLength:Int, train: (Seq[PathLabelVariable],Seq[VectorVariable],Seq[Double]) => M) extends TrainingPolicy {
        def train(trainingExamples: Seq[(PathLabelVariable,Double)]) = {
            val dim = trainingExamples.head._1.features.head.domain.dimensionSize
            val dom = new DiscreteDomain(maxPathLength*dim)
            val m = train(trainingExamples.map(_._1),
                trainingExamples.map(ex => new VectorVariable(PathClassifierFromModel.seqToFeature(ex._1.features, maxPathLength)){ def domain = dom}),
                trainingExamples.map(_._2))

            new PathClassifierFromModel(maxPathLength, m)
        }
    }

    trait EncodingPolicy {
        def pathToFeatureSeq(path:Seq[String], store:GraphPathStore):PathFeatureVariable
    }

    class OneOfNEncoding(domain:CategoricalVectorDomain[String] = new CategoricalVectorDomain[String]{}) extends FeatureToTensorEncoding(ft => {
            val idx = domain._dimensionDomain.index(ft)
            val tensor = new SparseIndexedTensor1(domain.dimensionSize)
            tensor.update(idx,1.0)
            tensor
        }, _.normalize() , domain)

    class FeatureToTensorEncoding(ftToTensor: String => Tensor1, postProcessing: Tensor1 => Unit , val domain:CategoricalVectorDomain[String]) extends EncodingPolicy {
        self =>

        //val featureMap = mutable.Map[String,Tensor1]()

        //val cacheDependencyPaths = mutable.Map[String,String]()

        def pathToFeatureSeq(path: Seq[String], store:GraphPathStore) = {
            if(path.size > 1) {
                new PathFeatureVariable(path.sliding(2).map{ case Seq(from,to) =>
                    new FeatureVectorVariable[String]() {
                        def domain = self.domain
                        set({
                            val filtered = store.getEdges(from,to).filterNot(_._1.matches(exclusionPattern))
                            if(!filtered.isEmpty)
                                filtered.map{ case (feature,count) =>
                                    val shortVersion = DependencyPath.removeAttributes(feature.replaceAll("""\^-1""",""), withNeg = true)
                                    val t = ftToTensor(shortVersion)
                                    t*=count; t
                                }.reduce(_ + _)
                            else null
                        })(null)
                        if(value != null) {
                            postProcessing(value)
                            if(!value.exists(_ > 0.0))
                                set(null)(null)
                        }
                    }
                }.toSeq, path)
            } else new PathFeatureVariable(Seq.empty[FeatureVectorVariable[String]],path)
        }
    }

    val trueDim = PathLabelDomain.index("true")
    val maxPath = 3
    var exclusionPattern = ""

    def main(args:Array[String]) {
        object Conf {
            var posExampleDir:File = null
            var negExampleDir:File = null
            var trainingPolicy:String = "hmm"
            var hmmNrOfLabels:Int = 8
            var encoding:String = "one_of_N"
            var encodingModel:File = null
            var outputFile:File = null
            var exclusionPattern:String = ""
        }
        val parser = new OptionParser[Conf.type]("Training Path Classifier") {
            opt[File]('p', "pos_examples").action{ case (x, c) =>
                c.posExampleDir = x; c }.text("directory of positive examples").required()
            opt[File]('n', "neg_examples").action{ case (x, c) =>
                c.negExampleDir = x; c }.text("directory of negative examples").required()
            opt[String]('t',"training").action{ case (x, c) =>
                c.trainingPolicy = x; c }.text("training policy: hmm(default), random_forest")
            opt[String]('e',"encoding").action{ case (x, c) =>
                c.encoding = x; c }.text("encoding policy: one_of_N(default), lda (together with -m pointing to clustering output model)")
            opt[File]('m',"model").action{ case (x, c) =>
                c.encodingModel = x; c }.text("model used for encoding")
            opt[Int]('l',"nr_labels").action{ case (x, c) =>
                c.hmmNrOfLabels = x; c }.text(s"only together with hmm, nr of hidden labels: ${Conf.hmmNrOfLabels}(default)")
            opt[String]('x',"exclusion").action{ case (x, c) =>
                c.exclusionPattern = s"""(${x.split(",").mkString("|")})(\\^-1)?"""; c }.text("exclusion pattern")
            opt[File]('o',"outputFile").action{ case (x, c) =>
                c.outputFile = x; c }.text("output file")
        }

        if(!parser.parse(args,Conf).isDefined) {
            throw new IllegalArgumentException("Wrong parameters")
            System.exit(-1)
        }
        exclusionPattern = Conf.exclusionPattern

        var encoding:EncodingPolicy = Conf.encoding match {
            case "lda" =>
                ClusterEdges.deserialize(new FileInputStream(Conf.encodingModel))
                val numTopics = ClusterEdges.phis.length
                val domain = new CategoricalVectorDomain[String]{
                    (0 until numTopics).foreach(i => this.dimensionDomain.index(i.toString))
                }
                new FeatureToTensorEncoding(f => {
                    if(ClusterEdges.WordDomain._indices.contains(f)) {
                        val index = ClusterEdges.WordDomain.index(f)
                        val t = Tensor.tabulate(numTopics)(i => ClusterEdges.phis(i).value(index)*ClusterEdges.topicProbs(i))
                        t.normalize()
                        t
                    } else
                        Tensor.tabulate(numTopics)(_=> 0.0)
                },_.normalize(),domain)
            case "lda_doc" =>
                val thetas = ClusterEdges.deserializeThetas(new FileInputStream(Conf.encodingModel))
                val numTopics = thetas.head._2.length
                val dom = new CategoricalVectorDomain[String]{
                    (0 until numTopics).foreach(i => this.dimensionDomain.index(i.toString))
                }
                new EncodingPolicy {
                    def pathToFeatureSeq(path: Seq[String], store: GraphPathStore): PathFeatureVariable = {
                        new PathFeatureVariable(path.sliding(2).map(pair => {
                            new FeatureVectorVariable[String]() {
                                if(store.getEdges(pair.head,pair(1)).exists(_._1.matches(exclusionPattern)))
                                    set(null)(null)
                                else {
                                    set(thetas.getOrElse(pair.mkString("-"),thetas.getOrElse(pair.reverse.mkString("-"),new SparseIndexedTensor1(numTopics))))(null)
                                    value.normalize()
                                }
                                def domain: CategoricalVectorDomain[String] = dom
                            }
                        }).toSeq, path)
                    }
                }
            case _ => new OneOfNEncoding
        }

        val posExamples = Conf.posExampleDir.listFiles().map(file => {
            val store = GraphPathStore.fromFile(file)
            val size = store.getPaths.size
            store.getPaths.withFilter(p => p.size > 2 && p.size <= maxPath).map(p => (new PathLabelVariable(true, encoding.pathToFeatureSeq(p,store)),1.0)).filterNot(v => v._1.features.isEmpty || v._1.features.exists(_.value == null))
        }).filterNot(_.isEmpty).toList
        val negExamples = Conf.negExampleDir.listFiles().map(file => {
            val store = GraphPathStore.fromFile(file)
            val size = store.getPaths.size
            store.getPaths.withFilter(p => p.size > 2 && p.size <= maxPath).map(p => (new PathLabelVariable(false, encoding.pathToFeatureSeq(p,store)), 1.0)).filterNot(v => v._1.features.isEmpty || v._1.features.exists(_.value == null))
        }).filterNot(_.isEmpty).toList

        encoding = null

        val totalPos = posExamples.size
        val totalNeg = negExamples.size
        val min = math.min(totalPos,totalNeg)

        val totalPaths = posExamples.map(_.size).sum + negExamples.map(_.size).sum

        val policy:TrainingPolicy = Conf.trainingPolicy match {
            //case "random_forest" => new NoiseReductionTraining(new ModelTraining[RandomForestMulticlassClassifier](maxPath-1, new RandomForestMulticlassTrainer(10,-1,min*4 /10)))
            case "random_forest" => new ModelTraining[RandomForestMulticlassClassifier](maxPath-1, new RandomForestMulticlassTrainer(1000,-1,totalPaths*2 /1000).train)
            case "id3" => new ModelTraining[DecisionTreeMulticlassClassifier](maxPath-1, new DecisionTreeMulticlassTrainer[Any]().train)
            case "linear" => new ModelTraining[VectorClassifier[PathLabelVariable,VectorVariable]](maxPath-1, (labels,features,weights) => {
                val map = labels.zip(features).toMap
                val trainer = new BatchOptimizingLinearVectorClassifierTrainer()
                trainer.train(labels,label => map(label))
            })
            case _ =>  new NoiseReductionTraining(new HMMTraining(Conf.hmmNrOfLabels))
        }

        val folds = 10

        val chunks = Random.shuffle(posExamples).take(min).grouped(min/folds).toList.zip(Random.shuffle(negExamples).take(min).grouped(min/folds).toList).take(folds)
        var ctr = 1
        val (trainAgg,trainBest,testAgg,testBest) = chunks.map(testChunk => {
            println(s"Fold $ctr of $folds...")
            ctr += 1

            val negTest = testChunk._2
            val posTest = testChunk._1

            val (posTrain,negTrain) = chunks.filter(_ != testChunk).reduce((acc,c) => (acc._1 ++ c._1, acc._2 ++ c._2))
            val pathClassifier = policy.train(posTrain.reduce(_ ++ _) ++ negTrain.reduce(_ ++ _))

            def aggregatedScore(ts:Seq[Tensor1]) = {
                new DenseTensor1(ts.map(_.expNormalized).reduce(_ + _).map(math.log).asArray)
            }

            def bestKAggregatedScore(ts:Seq[Tensor1],k:Int = 10) = {
                aggregatedScore(ts.sortBy(-_(trueDim)).take(k))
            }

            (runTrial(pathClassifier, posTrain.map(_.map(_._1)), negTrain.map(_.map(_._1)), aggregatedScore),
             runTrial(pathClassifier, posTrain.map(_.map(_._1)), negTrain.map(_.map(_._1)), ts => bestKAggregatedScore(ts, ts.size/10 +1)),
             runTrial(pathClassifier, posTest.map(_.map(_._1)), negTest.map(_.map(_._1)), aggregatedScore),
             runTrial(pathClassifier, posTest.map(_.map(_._1)), negTest.map(_.map(_._1)), ts => bestKAggregatedScore(ts, ts.size/10+1)) )

        }).reduce((acc,trials) => (acc._1 ++ trials._1,acc._2 ++ trials._2, acc._3 ++ trials._3, acc._4 ++ trials._4))

        //evaluateTrial("train: aggregated score", trainAgg)
        //evaluateTrial("train: best positive score", trainBest)
        //evaluateTrial("test: aggregated score", testAgg)
        evaluateTrial("test: best positive score", testBest, Conf.outputFile)
    }


    def runTrial(pathClassifier: PathClassifier, posTest: Seq[Seq[PathLabelVariable]],negTest: Seq[Seq[PathLabelVariable]],combiner:Seq[Tensor1]=> Tensor1,withOutput: Boolean = false) = {
        val classifier = new PairFromPathClassifier(pathClassifier, combiner, if(withOutput) out => {
            if(!out.isEmpty && out.head._1.intValue == trueDim) {
                println("best true path examples")
                out.sortBy(-_._2(trueDim)).take(5).foreach( o => println(o._2(trueDim)+"\t"+ o._1.features.path.mkString(" - ")) )
            }
        } else null)

        val trial = new Trial(classifier, PathLabelDomain, (l: PairLabelVariable) => l.paths)
        trial ++= posTest.map(ex => new PairLabelVariable(true, ex)) ++ negTest.map(ex => new PairLabelVariable(false, ex))

        trial.toIndexedSeq
    }

    def evaluateTrial(name: String,trial: Seq[LabeledClassification[PairLabelVariable]], outputFile:File = null) {
        println(s"Evaluating $name")
        val sortedTrials = trial.sortBy(c => c.classification.proportions(PathLabelDomain.index("false"))).toList
        val sorted = sortedTrials.map(c => (c.classification.proportions, c.label.target.categoryValue))

        if(outputFile == null) {
            val totalPosTest = trial.count(_.label.target.categoryValue == "true")
            val totalNegTest = trial.count(_.label.target.categoryValue == "false")

            println(s"Nr of positive examples: $totalPosTest")
            println(s"Nr of negative examples: $totalNegTest")

            var accFP = 0.0
            var accTP = 0.0

            val roc = (0.0, 0.0) :: sorted.map {
                case (prediction, label) =>
                    if (label == "false") {
                        accFP += 1
                        Some((accFP / totalNegTest) -> (accTP / totalPosTest))
                    } else {
                        accTP += 1
                        None
                    }
            }.flatten

            val precRec = roc.mapConserve(r => (r._2, r._2 * totalPosTest / (r._1 * totalNegTest + r._2 * totalPosTest)))

            import breeze.plot._

            val f = Figure(name)
            val p = f.subplot(0)
            val auc = roc.map(r => r._2/totalNegTest).sum
            p.title = "ROC"
            println("AUC: " +auc)
            p += plot(roc.map(_._1).toList, roc.map(_._2).toList)
            p += plot(List(0, 1), List(0, 1))
            p.xlim = (0.0,1.0)
            p.ylim = (0.0,1.0)
            val (rec, prec) = precRec.filterNot(_._1 < 0.1).maxBy(_._2)
            val p1 = f.subplot(2,2,1)
            p1.title = "Precision, Recall"
            p1.xlim = (0.0,1.0)
            p1.ylim = (totalPosTest.toDouble/(totalPosTest+totalNegTest), 1)
            println(s"Best Precision at (with recall at least 0.1): $prec ($rec)")
            p1 += plot(precRec.map(_._1).toList, precRec.map(_._2).toList)
            val p2 = f.subplot(2)
            p2.title = "True prob distr"
            p2.xlim = (0.0,1.0)
            p2 += hist(sorted.withFilter(_._2 == "true").map(_._1(PathLabelDomain.index("true"))), 100)
            val p3 = f.subplot(3)
            p3.title = "False prob distr"
            p3.xlim = (0.0,1.0)
            p3 += hist(sorted.withFilter(_._2 == "false").map(_._1(PathLabelDomain.index("true"))), 100)
        } else {
            val pw = new PrintWriter(new FileWriter(outputFile))

            sorted.foreach{ case (proportions,label) => pw.println(proportions(PathLabelDomain.index("true"))+ "\t" + { if(label=="true") "1" else "0" })}

            pw.close()
        }
    }
}


