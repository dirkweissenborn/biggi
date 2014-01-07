package biggi.classify

import cc.factorie.variable._
import cc.factorie.app.chain.{ Observation}
import cc.factorie.la._
import cc.factorie.infer.{InferByBPChain}
import biggi.model.factorie.HMMModel
import cc.factorie.optimize.Trainer

/**
 * @author dirk
 *          Date: 12/10/13
 *          Time: 10:07 AM
 */
object HmmExample {
    implicit val rand = new scala.util.Random

    val numLabels = 2
    val seqLength = 10

    lazy val model = new HMMModel(numLabels, FeaturesDomain)

    object FeaturesDomain extends CategoricalVectorDomain[String]

    def main(args:Array[String]): Unit = {
        val transitions = Map[String,Map[String,Double]](
            "rainy" -> Map[String,Double]("rainy" -> 0.8,"sunny" -> 0.2),//"cloudy" -> 0.3),
            //"cloudy" -> Map[String,Double]("rainy" -> 0.2,"sunny" -> 0.2,"cloudy" -> 0.6),
            "sunny" -> Map[String,Double]("rainy" -> 0.3,"sunny" -> 0.7))//,"cloudy" -> 0.3))
        val totalTransitions = transitions.map(_._2).reduce((acc,map) => {
            map.map(key => key._1 -> (acc(key._1) + key._2))
        }).mapValues(_ / transitions.size)
        val emmissionProbs = Map[String,Map[String,Double]](
            "rainy" -> Map[String,Double]("walk" -> 0.1,"shop" -> 0.6, "clean" -> 0.3),
            //"cloudy" -> Map[String,Double]("walk" -> 0.1,"shop" -> 0.3, "clean" -> 0.6),
            "sunny" -> Map[String,Double]("walk" -> 0.6,"shop" -> 0.3, "clean" -> 0.1))

        val totalEmissions = emmissionProbs.map(_._2).reduce((acc,map) => {
            map.map(key => key._1 -> (acc(key._1) + key._2))
        }).mapValues(_ / emmissionProbs.size)

        def sampleExample(write:Boolean = true, positive:Boolean = true) = {
            def sampleLabel = {
                val r = rand.nextDouble()
                if(r <= 0.5) ("rainy",0.3,0.3) else /*if(r <=0.6) ("cloudy",0.3,0.3) else*/ ("sunny",0.3,0.3)
            }
            def sampleObservation(label:String) = {
                val s = rand.nextDouble
                var acc = 0.0
                val ems = { if(positive) emmissionProbs(label)
                  else totalEmissions }
                val sample = ems.dropWhile(el => {
                    acc += el._2
                    acc <= s
                }).head
                (sample._1,sample._2,totalEmissions(sample._1))
            }
            def sampleTransition(label:String) = {
                val s = rand.nextDouble()
                var acc = 0.0
                val trs = { if(positive) transitions(label)
                else totalTransitions }
                val sample = trs.dropWhile(el => {
                    acc += el._2
                    acc <= s
                }).head

                (sample._1,sample._2,1.0)
            }

            var current = sampleLabel
            val lbls = (0 until seqLength).map(_ => {
                val token = sampleObservation(current._1)
                if(write)
                    print(current._1+":"+token._1+"\t")
                val l = new model.Features(List(token._1))
                current = sampleTransition(current._1)
                l
            }).toSeq
            if(write)
                println()

            lbls
        }

        def printLabeledExample(ftrs:Seq[model.Features]) {
            println(ftrs.map(ft => ft.label.intValue+":"+FeaturesDomain._dimensionDomain.category(ft.value.maxIndex)).mkString("\t"))
        }

        val examples = (0 until 10000).map(i => sampleExample(i < 5)).toSeq

        /*val neg_examples = (0 until 100000).map(i => sampleExample(i<0,false)).toSeq

        var map = examples.map(_.map(_.token.featureStrings.head).mkString(" ")).foldLeft(Map[String,Int]())((acc,el) => acc + (el -> (acc.getOrElse(el,0)+1)))
        map = neg_examples.map(_.map(_.token.featureStrings.head).mkString(" ")).foldLeft(map)((acc,el) => acc + (el -> (acc.getOrElse(el,0)-1)))



        val f = Figure()
        val p = f.subplot(0)
        p += hist(map.values.toSeq,10000)
        p.title = "distribution"  */

        def printModelParameters {
            for (i <- 0 until model.markov.weights.value.dim1) {
                println(i)
                var sum = 0.0
                for (j <- 0 until model.markov.weights.value.dim2)
                    sum += math.exp(model.markov.weights.value.apply(i, j))
                for (j <- 0 until model.markov.weights.value.dim2) print((math.exp(model.markov.weights.value.apply(i, j)) / sum) + "\t")
                println()
            }

            for (i <- 0 until model.obs.weights.value.dim1) {
                println(i)
                var sum = 0.0
                for (j <- 0 until model.obs.weights.value.dim2)
                    sum += math.exp(model.obs.weights.value.apply(i, j))
                for (j <- 0 until model.obs.weights.value.dim2)
                    print(FeaturesDomain._dimensionDomain.category(j) + ":" + (math.exp(model.obs.weights.value.apply(i, j)) / sum) + "\t")

                println()
            }
        }

        Trainer.batchTrain(model.parameters,
            examples.map(ex => new model.HMMExample(ex)),
            optimizer = new model.HMMOptimizer,
            useParallelTrainer = true,
            maxIterations = 10)

        printModelParameters

        examples.take(5).foreach(e => {
            val summary = InferByBPChain(e.map(_.label),model)
            summary.setToMaximize(null)
            printLabeledExample(e)
        })
    }
}

