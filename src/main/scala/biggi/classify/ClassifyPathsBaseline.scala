package biggi.classify

import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import cc.factorie.app.classify._
import cc.factorie.app.classify.backend.{BatchLinearMulticlassTrainer, OnlineLinearMulticlassTrainer}
import cc.factorie.tutorial.DocumentClassifier1
import cc.factorie.variable._
import scala.io.Source


/**
 * @author dirk
 *          Date: 12/6/13
 *          Time: 11:25 AM
 */
object  ClassifyPathsBaseline {

    // Define variable classes
    object DocumentDomain extends CategoricalVectorDomain[String]
    DocumentDomain._dimensionDomain.gatherCounts = true
    class Document(file:File) extends BinaryFeatureVectorVariable[String] {
        def domain = DocumentDomain
        val label = new Label(file.getParentFile.getName, this)
        Source.fromFile(file).getLines.foreach(line => {
            val token = line.replaceAll("""\([^)}]*\)""","")
            if((line.contains("->") || line.contains("<-")) && !line.contains("SIB") &&
                (!DocumentDomain._dimensionDomain._indices.contains(token) || !this.contains(DocumentDomain._dimensionDomain.getIndex(token))) )
                this += token
        })
    }
    object LabelDomain extends CategoricalDomain[String]
    class Label(labelString:String, val document:Document) extends LabeledCategoricalVariable(labelString) {
        def domain = LabelDomain
    }

    def main(args:Array[String]): Unit = {
        implicit val random = new scala.util.Random(0)
        if (args.length < 2)
            throw new Error("Usage: directory_class1 directory_class2 ...\nYou must specify at least two directories containing text files for classification.")

        // Read data and create Variables
        var docLabels = new ArrayBuffer[Label]()
        for (directory <- args) {
            val directoryFile = new File(directory)
            if (! directoryFile.exists) throw new IllegalArgumentException("Directory "+directory+" does not exist.")
            for (file <- new File(directory).listFiles; if file.isFile) {
                docLabels += new Document(file).label
            }
        }

        val (testVariables, trainVariables) = Random.shuffle(docLabels).splitAt(docLabels.size/10)
        (trainVariables ++ testVariables).foreach(_.setRandomly)

        val valueTrainers = Seq(
            new OnlineLinearMulticlassTrainer(),
            new BatchLinearMulticlassTrainer()
        )
        val trainers = Seq(
            new OnlineOptimizingLinearVectorClassifierTrainer(),
            new BatchOptimizingLinearVectorClassifierTrainer(),
            new NaiveBayesClassifierTrainer()
            //new ID3DecisionTreeClassifier()
        )
        for (trainer <- valueTrainers) {
            println(trainer.getClass)
            val classifier = trainer.train(trainVariables, trainVariables.map(_.document))
            (trainVariables ++ testVariables).foreach(label => { label := classifier.classification(label.document.value).bestLabelIndex })
            println ("Train accuracy = "+ HammingObjective.accuracy(trainVariables))
            println ("Test accuracy = "+ HammingObjective.accuracy(testVariables))
        }
        for (trainer <- trainers) {
            println(trainer.getClass)
            val classifier = trainer.train(trainVariables, (label:Label)=>label.document)
            println ("Train accuracy = "+ classifier.accuracy(trainVariables))
            println ("Test accuracy = "+ classifier.accuracy(testVariables))
        }

        DocumentDomain._dimensionDomain.map(catVal => (catVal.domain.count(catVal.category), catVal.category)).filter(_._1 > 1).sortBy(-_._1).take(100).foreach(println)

    }

}
