package biggi.classify

import java.io.{FileWriter, PrintWriter, FileInputStream, File}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import cc.factorie.app.classify._
import cc.factorie.app.classify.backend.{MulticlassClassifierTrainer, MulticlassClassifier, BatchLinearMulticlassTrainer, OnlineLinearMulticlassTrainer}
import cc.factorie.tutorial.DocumentClassifier1
import cc.factorie.variable._
import scala.io.Source
import biggi.model.deppath.{DependencyPath, GraphPathStore}
import scala.collection.JavaConversions._
import scala.Some
import cc.factorie.app.classify.LabeledClassification
import scala.Some
import cc.factorie.app.classify.LabeledClassification
import cc.factorie.la.{DenseTensor1, Tensor, SparseIndexedTensor1, Tensor1}
import biggi.cluster.{GPCA, ClusterEdges}
import scala.collection.mutable


/**
 * @author dirk
 *          Date: 12/6/13
 *          Time: 11:25 AM
 */
object  ClassifyPathsBaseline {

    // Define variable classes
    object DocumentDomain extends CategoricalVectorDomain[String]
    DocumentDomain._dimensionDomain.gatherCounts = true
    class Document(tokens:TraversableOnce[String],positive:Boolean,val name:String = "") extends FeatureVectorVariable[String] {
        def domain = DocumentDomain
        val label = new Label(positive.toString, this)

        tokens.foreach(token => this += token)
    }
    object LabelDomain extends CategoricalDomain[String] {
        index("true")
        index("false")
    }
    class Label(labelString:String, val document:Document) extends LabeledCategoricalVariable(labelString) {
        def domain = LabelDomain
    }

    private trait PairEncoder {
        def pairToExample(store:GraphPathStore, positive:Boolean):Label
    }

    private class OneOfNEncoder(minLength:Int, maxLength:Int,exclusionPattern:String) extends PairEncoder {
        def pairToExample(store: GraphPathStore, positive: Boolean) = {
            val paths = store.getPaths.withFilter(p => p.size >= minLength && p.size <= maxLength).flatMap(path => path.sliding(2).foldLeft(List[String](""))((acc,pair) => {
                val m = store.getEdges(pair(0),pair(1))
                acc.flatMap(partial => m.withFilter(e => !e._1.matches(exclusionPattern)).flatMap(entry => (0 until entry._2).map(_ => partial +"\t"+DependencyPath.removeAttributes(entry._1, withNeg = true))))
            }))

            val doc = new Document(paths,positive)
            //doc.value.normalize()
            doc.label
        }
    }

    private class LDAEncoder(minLength:Int, maxLength:Int,exclusionPattern:String, modelFile:File) extends PairEncoder {
        ClusterEdges.deserialize(new FileInputStream(modelFile))
        val numTopics = ClusterEdges.phis.length
        /*val cuiToSts = Source.fromFile("/home/bioinf/dirkw/cuiToSts").getLines().map(line => {
            val split = line.split("\t")
            split(0) -> split(1).split(",").toSet
        }).toMap*/

        def pairToExample(store: GraphPathStore, positive: Boolean) = {
            val paths = store.getPaths.withFilter(p => p.size >= minLength && p.size <= maxLength).flatMap(path => path.sliding(2).foldLeft(List[(String,Double)](("",1.0)))((acc,pair) => {
                /*val semtypes2 = cuiToSts.getOrElse(pair(1),Set[String]())
                val semTypes = cuiToSts.getOrElse(pair(0),Set[String]()).flatMap(st1 => semtypes2.map(st2 => if(st1 < st2) "semtypes:" + st1+"-"+st2 else  "semtypes:" + st2+"-"+st1))

                val typeTensor =  Tensor.tabulate(numTopics)(i => if(semTypes.isEmpty)
                    Double.NegativeInfinity
                else {
                    val values = semTypes.map(typePair => {
                        val idx = ClusterEdges.WordDomain.index(typePair)
                        if(idx >= 0)
                            ClusterEdges.phis(i).value(idx)
                        else
                            0.0
                    }).filterNot(_ == 0.0)
                    if(values.isEmpty) Double.NegativeInfinity else math.log(values.reduce(_ * _))
                })*/
                if(!acc.isEmpty) {
                    val tensor = store.getEdges(pair.head,pair(1)).foldLeft[Tensor1](Tensor.tabulate(numTopics)(i => 0.0))((acc,edge) => {
                        val f = DependencyPath.removeAttributes(edge._1.replaceAll("""\^-1""",""),withNeg=true)
                        if(!f.matches(exclusionPattern) && ClusterEdges.WordDomain._indices.contains(f))
                            acc += {
                                    val index = ClusterEdges.WordDomain.index(f)
                                    val t = Tensor.tabulate(numTopics)(i => ClusterEdges.phis(i).value(index) * ClusterEdges.topicProbs(i))
                                    t.normalize()
                                    if(edge._2 > 1)
                                        t *= edge._2
                                    t
                                 }
                        acc
                    })

                    //if(typeTensor.exists(_ > Double.NegativeInfinity))
                     //   tensor += typeTensor

                    tensor.normalize()

                    var result = List[(String,Double)]()
                    acc.foreach{ case (partial,score) =>
                        tensor.foreachActiveElement{ case (idx,s) => if(s >= 0.01) result ::= (partial +"\t"+ idx, score *s) }
                    }

                    result
                }
                else acc
            }))


            val pname = store.getPaths.head.head+"-"+store.getPaths.head.last

            if(paths.isEmpty)
                println(pname)

            val doc = new Document(List[String](),positive, name = pname)
            paths.foreach(path => doc += (path._1,path._2))
            if(paths.isEmpty)
                println(pname)

            doc.label
        }
    }

    private class GPCAEncoder(minLength:Int, maxLength:Int, modelFile:File) extends PairEncoder {
        val vectors = GPCA.deserialize(new FileInputStream(modelFile))

        def pairToExample(store: GraphPathStore, positive: Boolean) = {
            val paths = store.getPaths.withFilter(p => p.size >= minLength && p.size <= maxLength).flatMap(path => {
                val p1 = path.take(2)
                val p2 = path.tail
                val t1 = vectors.getOrElse(p1.sorted.mkString("-"),{println("Didn't find vector for "+p1.sorted.mkString("-"));Tensor.tabulate(vectors.head._2.length)(_=> 0.0)})
                val t2 = vectors.getOrElse(p2.sorted.mkString("-"),{println("Didn't find vector for "+p2.sorted.mkString("-"));Tensor.tabulate(vectors.head._2.length)(_=> 0.0)})

                var result = List[(String,Double)]()
                (0 until t1.dim1).foreach(dim => {
                    val (v1,v2) = (t1(dim),t2(dim))
                    if(v1 < 0)
                        if(v2 < 0) {
                            result ::= (dim+"_1",- v1*v2)
                            result ::= (dim+"_2", v1*v2)
                        } else {
                            result ::= (dim+"_2", v1*v2)
                            result ::= (dim+"_3", v1*v2)
                        }
                    else
                        if(v2 >= 0) {
                            result ::= (dim+"_1", v1*v2)
                            result ::= (dim+"_2", v1*v2)
                        } else {
                            result ::= (dim+"_2", v1*v2)
                            result ::= (dim+"_3", -v1*v2)
                        }
                })
                result
            })
            val doc = new Document(List[String](),positive)
            paths.foreach(path => doc += (path._1,path._2))
            doc.label
        }
    }

    def main(args:Array[String]): Unit = {
        implicit val random = new scala.util.Random(0)

        val trainer = () => new BatchOptimizingLinearVectorClassifierTrainer()

        val posExamplesDir = new File(args(0))
        val negExamplesDir = new File(args(1))

        val minLength = args(2).toInt
        val maxLength = args(3).toInt

        val excludeRels = args(4).split(",")
        val folds = args(5).toDouble
        println(s"$folds-crossvalidation")

        val encodingPolicy = args(6)
        var outFile:File = null

        val exclusionPattern = s"""(${excludeRels.mkString("|")})(\\^-1)?"""
        println("Exclusion pattern: "+exclusionPattern)

        val encoder:PairEncoder = encodingPolicy match {
            case "lda" => if(args.size > 8) outFile = new File(args(8)); new LDAEncoder(minLength,maxLength,exclusionPattern,new File(args(7)))
            case "gpca" => if(args.size > 8) outFile = new File(args(8)); new GPCAEncoder(minLength,maxLength, new File(args(7)))
            case _ => if(args.size > 7) outFile = new File(args(7)); new OneOfNEncoder(minLength,maxLength,exclusionPattern)
        }

        var ctr = -1
        // Read data and create variables
        val posExamples = posExamplesDir.listFiles().map(file => {
            val store = GraphPathStore.fromFile(file)
            ctr += 1
            if(ctr % 10 == 0)
                println(ctr+" positive examples encoded")
            encoder.pairToExample(store,true)
        }).filterNot(_.document.value.activeElements.isEmpty).toList

        ctr = -1
        val negExamples = negExamplesDir.listFiles().map(file => {
            val store = GraphPathStore.fromFile(file)
            ctr += 1
            if(ctr % 10 == 0)
                println(ctr+" negative examples encoded")
            encoder.pairToExample(store,false)
        }).filterNot(_.document.value.activeElements.isEmpty).toList

        val totalPos = posExamples.size
        val totalNeg = negExamples.size
        val min = math.min(totalPos,totalNeg)

        posExamples.foreach(_.setRandomly)
        negExamples.foreach(_.setRandomly)
        val realFolds = if(folds < 1) (1.0/folds).toInt else folds.toInt

        val chunks = Random.shuffle(posExamples).grouped(totalPos/realFolds).toList.zip(Random.shuffle(negExamples).grouped(totalNeg/realFolds).toList).take(realFolds)

        def crossvalidate(name:String, train:Seq[Label] => MulticlassClassifier[Tensor1]) {
            var trials = Seq[LabeledClassification[Label]]()
            var testTrials = Seq[LabeledClassification[Label]]()
            var ctr = 1
            chunks.foreach(testChunk => {
                println(s"Fold $ctr of $realFolds...")
                ctr += 1

                val negTest = testChunk._2
                val posTest = testChunk._1

                val (posTrain,negTrain) = chunks.filter(_ != testChunk).reduce((acc,c) => (acc._1 ++ c._1, acc._2 ++ c._2))

                val trainVariables = if(folds >= 1.0) Random.shuffle(posTrain ++ negTrain) else Random.shuffle(posTest ++ negTest)
                val testVariables = if(folds < 1.0) Random.shuffle(posTrain ++ negTrain) else Random.shuffle(posTest ++ negTest)
                val classifier = train(trainVariables)

                val trial = new Trial[Label,Tensor1](classifier,LabelDomain, _.document.value)
                trial ++= trainVariables

                val testTrial = new Trial[Label,Tensor1](classifier,LabelDomain, _.document.value)
                testTrial ++= testVariables

                trials ++= trial
                testTrials ++= testTrial
            })

            //evaluateTrial("train: "+name,trials)
            evaluateTrial("test: "+name,testTrials,outFile)// new File("/home/bioinf/dirkw/Dropbox/Diplom/paper/diagrams/length_3/log_reg/predictions.tsv"))
        }

        val training = trainer()
        crossvalidate(training.toString, labels => trainer().train(labels,(label:Label) => label.document).asInstanceOf[MulticlassClassifier[Tensor1]])


        val classifier = trainer().train(posExamples ++ negExamples, (label:Label) => label.document)

        val trueDim = LabelDomain.getIndex("true")
        val falseDim = LabelDomain.getIndex("false")
        val priorityQueue = new mutable.PriorityQueue[(Int,Double)]()(new Ordering[(Int,Double)]() {
            def compare(x: (Int, Double), y: (Int, Double)): Int = math.signum(x._2 - y._2).toInt
        })

        val numExampleFeatures = 100
        val weights = classifier.weights.value
        weights.foreachElement((idx, score) => {
            val (ft,lbl) = weights.multiIndex(idx)
            if(lbl == trueDim) {
                val s = score - weights(ft,falseDim)
                if(priorityQueue.size < numExampleFeatures)
                    priorityQueue.enqueue((ft, s))
                else if(priorityQueue.head._2 < s) {
                    priorityQueue.dequeue()
                    priorityQueue.enqueue((ft,s))
                }
            }
        })

        priorityQueue.dequeueAll.sortBy(_._2).foreach{ case (elIdx,score) =>
            val el = DocumentDomain._dimensionDomain(elIdx)
            println(s"$el\t$score")
        }
    }

    def evaluateTrial(name: String,trial: Seq[LabeledClassification[Label]], outputFile:File = null) {
        println(s"Evaluating $name")
        val sortedTrials = trial.sortBy(c => c.classification.proportions(LabelDomain.index("false"))).toList

        println("Bad positive examples")
        sortedTrials.filter(_.label.target.categoryValue ==  "true").takeRight(20).foreach(l => println(l.label.document.name))

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

            val precRec = roc.tail.mapConserve(r => (r._2, r._2 * totalPosTest / (r._1 * totalNegTest + r._2 * totalPosTest)))

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
            p.setXAxisDecimalTickUnits()
            p.setYAxisDecimalTickUnits()
            val (rec, prec) = precRec.filterNot(_._1 < 0.1).maxBy(_._2)
            val beta = 0.5
            val bestFMeasure = precRec.map(p => p._1*p._2*(1+beta*beta)/((1+beta*beta)*p._2+p._1)).max

            val p1 = f.subplot(2,2,1)
            p1.title = "Precision, Recall"
            p1.xlim = (0.0,1.0)
            p1.ylim = (totalPosTest.toDouble/(totalPosTest+totalNegTest), 1.0)
            p1.setXAxisDecimalTickUnits()
            p1.setYAxisDecimalTickUnits()
            println(s"Best Precision at (with recall at least 0.1): $prec ($rec)")
            println(s"Best F0.5 : $bestFMeasure")
            p1 += plot(precRec.map(_._1).toList, precRec.map(_._2).toList)
            val p2 = f.subplot(2)
            p2.title = "True prob distr"
            p2.xlim = (0.0,1.0)
            p2 += hist(sorted.withFilter(_._2 == "true").map(_._1(PathLabelDomain.index("true"))), 100)
            p2.setXAxisDecimalTickUnits()
            p2.setYAxisDecimalTickUnits()
            val p3 = f.subplot(3)
            p3.title = "False prob distr"
            p3.xlim = (0.0,1.0)
            p3 += hist(sorted.withFilter(_._2 == "false").map(_._1(PathLabelDomain.index("true"))), 100)
            p3.setXAxisDecimalTickUnits()
            p3.setYAxisDecimalTickUnits()
        }
        else {
            val pw = new PrintWriter(new FileWriter(outputFile))
            sorted.foreach{ case (proportions,label) => pw.println(proportions(PathLabelDomain.index("true"))+ "\t" + { if(label=="true") "1" else "0" })}
            pw.close()
        }
    }

}
