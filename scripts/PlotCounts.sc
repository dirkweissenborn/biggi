/**
 * @author dirk
 *          Date: 12/8/13
 *          Time: 1:14 PM
 */
import breeze.plot._
import java.io.File
import scala.io.Source
def rocFromFile(f:File) = {
    var accFP = 0.0
    var accTP = 0.0
    var trueProbs = List[Double]()
    var negProbs = List[Double]()
    var roc = (0.0 -> 0.0) :: Source.fromFile(f).getLines().map(line => {
        val Array(pred,label) = line.split("\t",2)
        if (label == "0") {
            accFP += 1
            negProbs ::= pred.toDouble
            Some(accFP -> accTP)
        } else {
            accTP += 1
            trueProbs ::= pred.toDouble
            None
        }
    }).flatten.toList

    roc = roc.mapConserve{ case (fp,tp) => (fp/accFP,tp/accTP)}

    (roc, roc.dropWhile(_._2 == 0.0).mapConserve(r => (r._2, r._2 * accTP / (r._1 * accFP + r._2 * accTP))), trueProbs, negProbs)
}
def getAUC(roc: List[(Double, Double)]): Double = {
    val total = roc.size
    val auc = roc.map(r => r._2 / total).sum
    auc
}
def printStats(name:String, roc:List[(Double,Double)], precRec:List[(Double,Double)]) {
    val auc: Double = getAUC(roc)
    println("Stats for "+name)
    println("AUC: "+ auc)
    val bestAcc = roc.maxBy(el =>el._2-el._1)
    val bestPrecRec = precRec.find(_._1 == bestAcc._2).get
    println("Best Accuracy: "+(bestAcc._2+1-bestAcc._1)/2+"("+bestPrecRec._2+","+bestPrecRec._1+")")
    val (rec, prec) = precRec.filterNot(_._1 < 0.3).maxBy(_._2)
    println(s"Best Precision at (with recall at least 0.3): $prec ($rec)")
    println()
}
val relation = "has_target"
val dir = new File("/home/bioinf/dirkw/results0.001_0.001/"+relation)
val imgDir = new File("/home/bioinf/dirkw/Dropbox/Diplom/paper/images/"+relation)
val hmm = rocFromFile(new File(dir,"hmm_path_length3.tsv"))
printStats("hmm",hmm._1,hmm._2)
val hmmLDA = rocFromFile(new File(dir,"hmm_path_lda_length3.tsv"))
printStats("hmm LDA",hmmLDA._1,hmmLDA._2)

val logRegPath = rocFromFile(new File(dir,"log_reg_path_length3.tsv"))
printStats("log reg path",logRegPath._1,logRegPath._2)
val logRegPathLDA = rocFromFile(new File(dir,"log_reg_path_lda_length3.tsv"))
printStats("log reg path, LDA",logRegPathLDA._1,logRegPathLDA._2)
val (length3_ROC,length3_precRec, length3_trueProbs, length3_falseProbs) = rocFromFile(new File(dir,"log_reg_length3.tsv"))
printStats("length 3",length3_ROC,length3_precRec)





val (length3Lda_ROC,length3Lda_precRec, length3Lda_trueProbs, length3Lda_falseProbs) = rocFromFile(new File(dir,"log_reg_lda_length3.tsv"))
printStats("length 3 with lda",length3Lda_ROC,length3Lda_precRec)



//val (length3_path_ROC,length3_path_precRec, length3_path_trueProbs, length3_path_falseProbs) = rocFromFile(new File(dir,"hmm_path_length3.tsv"))
//printStats("length 3, path",length3_path_ROC,length3_path_precRec)
//val (length3_pathLda_ROC,length3_pathLda_precRec, length3_pathLda_trueProbs, length3_pathLda_falseProbs) = rocFromFile(new File(dir,"hmm_path_lda_length3.tsv"))
//printStats("length 3 with lda, path",length3_pathLda_ROC,length3_pathLda_precRec)
val (length4_ROC,length4_precRec, length4_trueProbs, length4_falseProbs) = rocFromFile(new File(dir,"log_reg_length4.tsv"))
printStats("length 4",length4_ROC,length4_precRec)





val (length4Lda_ROC,length4Lda_precRec, length4Lda_trueProbs, length4Lda_falseProbs) = rocFromFile(new File(dir,"log_reg_lda_length4.tsv"))
printStats("length 4 with lda",length4Lda_ROC,length4Lda_precRec)
//Length comparison
var f = Figure()
f.height = f.width
var p = f.subplot(0)
p += plot(length3_ROC.map(_._1).toList, length3_ROC.map(_._2).toList, name = "length3")
p += plot(length4_ROC.map(_._1).toList, length4_ROC.map(_._2).toList, name = "length4")
p += plot(length3Lda_ROC.map(_._1).toList, length3Lda_ROC.map(_._2).toList, name = "length3, lda")
p += plot(length4Lda_ROC.map(_._1).toList, length4Lda_ROC.map(_._2).toList, name = "length4, lda")
p += plot(List(0, 1), List(0, 1), name ="", colorcode = "black")
p.xlim = (0.0,1.0)
p.ylim = (0.0,1.0)
p.setXAxisDecimalTickUnits()
p.setYAxisDecimalTickUnits()
p.xlabel = "false-positive rate"
p.ylabel = "true-positive rate"
p.title = "ROC"
p.legend = true
f.saveas(new File(imgDir,"roc_length.png").getAbsolutePath, dpi = 300)
/*f = Figure()
f.height = f.width
p = f.subplot(0)
p += plot(List(2002.0,2003.0,2004.0,2007.0,2009.0,2010.0,2011.0,2012.0,2013.0), List(0.777,0.875,1,1.3,2.125,2.201,2.405,2.67,2.919))
p.xlim = (2002.0,2014.0)
p.ylim = (0.0,3.0)
p.setYAxisDecimalTickUnits()
p.ylabel = "Millions of Concepts"
p.xlabel = "Year"
p.title = "
import java.awt.{Color, Font}
val legenFont = new Font("legend",Font.PLAIN, 14)
p.xaxis.setLabelFont(legenFont)
p.yaxis.setLabelFont(legenFont)
p.xaxis.setTickLabelFont(legenFont)
p.yaxis.setTickLabelFont(legenFont)"*/
val (length2_2_ROC,length2_2_precRec, length2_2_trueProbs, length2_2_falseProbs) = rocFromFile(new File(dir,"log_reg_length2-2.tsv"))
printStats("length 2-2",length2_2_ROC,length2_2_precRec)
val (length2_3_ROC,length2_3_precRec, length2_3_trueProbs, length2_3_falseProbs) = rocFromFile(new File(dir,"log_reg_length2-3.tsv"))
printStats("length 2-3",length2_3_ROC,length2_3_precRec)

val (length2_2Lda_ROC,length2_2Lda_precRec, length2_2Lda_trueProbs, length2_2Lda_falseProbs) = rocFromFile(new File(dir,"log_reg_lda_length2-2.tsv"))
printStats("length 2-2, lda",length2_2Lda_ROC,length2_2Lda_precRec)

val (length2_3Lda_ROC,length2_3Lda_precRec, length2_3Lda_trueProbs, length2_3Lda_falseProbs) = rocFromFile(new File(dir,"log_reg_lda_length2-3.tsv"))
printStats("length 2-3, lda",length2_3Lda_ROC,length2_3Lda_precRec)

f = Figure()
f.height = f.width
p = f.subplot(0)
p += plot(length2_2_ROC.map(_._1).toList, length2_2_ROC.map(_._2).toList, name = "length 2-2")
p += plot(length2_3_ROC.map(_._1).toList, length2_3_ROC.map(_._2).toList, name = "length 2-3")
//p += plot(length2_2Lda_ROC.map(_._1).toList, length2_2Lda_ROC.map(_._2).toList, name = "length 2-2, lda")
//p += plot(length2_3Lda_ROC.map(_._1).toList, length2_3Lda_ROC.map(_._2).toList, name = "length 2-3, lda")
p += plot(List(0, 1), List(0, 1), name ="", colorcode = "black")
p.xlim = (0.0,1.0)
p.ylim = (0.0,1.0)
p.setXAxisDecimalTickUnits()
p.setYAxisDecimalTickUnits()
p.xlabel = "false-positive rate"
p.ylabel = "true-positive rate"
p.title = "ROC"
p.legend = true
f.saveas(new File(imgDir,"roc_length_including_2.png").getAbsolutePath, dpi = 300)
//precision-recall
f = Figure()
f.height = f.width
p = f.subplot(0)
p += plot(length3_precRec.map(_._1).toList, length3_precRec.map(_._2).toList, name = "length3")
p += plot(length4_precRec.map(_._1).toList, length4_precRec.map(_._2).toList, name = "length4")
p += plot(length3Lda_precRec.map(_._1).toList, length3Lda_precRec.map(_._2).toList, name = "length3, lda")
p += plot(length4Lda_precRec.map(_._1).toList, length4Lda_precRec.map(_._2).toList, name = "length4, lda")
p.xlim = (0.0,1.0)
p.ylim = (0.5,1.0)
p.setXAxisDecimalTickUnits()
p.setYAxisDecimalTickUnits()
p.xlabel = "Recall"
p.ylabel = "Precision"
p.title = "Precision-Recall Curve"
p.legend = true
f.saveas(new File(imgDir,"precRec_length.png").getAbsolutePath, dpi = 300)
//prob distr comparison
f = Figure()
f.height = f.width
p = f.subplot(0)
p += hist(length3_trueProbs, 100, name = "plain")
p += hist(length3Lda_trueProbs, 100, name = "lda")
p.xlim = (0.0,1.0)
p.setXAxisDecimalTickUnits()
p.setYAxisDecimalTickUnits()
p.xlabel = "Probability"
p.ylabel = "nr. of examples"
p.title = "Positive Probability Distributions"
p.legend = true
//lda vs. plain, different amounts of training data
if(dir.list().exists(_.endsWith("0.1fold.tsv"))) {
    val (length3_ROC_5,length3_precRec_5, length3_trueProbs_5, length3_falseProbs_5) = rocFromFile(new File(dir,"log_reg_length3_5fold.tsv"))
    printStats("length 3, 5-fold",length3_ROC_5,length3_precRec_5)
    val (length3Lda_ROC_5,length3Lda_precRec_5, length3Lda_trueProbs_5, length3Lda_falseProbs_5) = rocFromFile(new File(dir,"log_reg_lda_length3_5fold.tsv"))
    printStats("length 3 with lda, 5-fold",length3Lda_ROC_5,length3Lda_precRec_5)
    val (length3_ROC_2,length3_precRec_2, length3_trueProbs_2, length3_falseProbs_2) = rocFromFile(new File(dir,"log_reg_length3_2fold.tsv"))
    printStats("length 3, 2-fold",length3_ROC_2,length3_precRec_2)
    val (length3Lda_ROC_2,length3Lda_precRec_2, length3Lda_trueProbs_2, length3Lda_falseProbs_2) = rocFromFile(new File(dir,"log_reg_lda_length3_2fold.tsv"))
    printStats("length 3 with lda, 2-fold",length3Lda_ROC_2,length3Lda_precRec_2)
    val (length3_ROC_025,length3_precRec_025, length3_trueProbs_025, length3_falseProbs_025) = rocFromFile(new File(dir,"log_reg_length3_0.25fold.tsv"))
    printStats("length 3, 0.25-fold",length3_ROC_025,length3_precRec_025)
    val (length3Lda_ROC_025,length3Lda_precRec_025, length3Lda_trueProbs_025, length3Lda_falseProbs_025) = rocFromFile(new File(dir,"log_reg_lda_length3_0.25fold.tsv"))
    printStats("length 3 with lda, 0.25-fold",length3Lda_ROC_025,length3Lda_precRec_025)
    val (length3_ROC_01,length3_precRec_01, length3_trueProbs_01, length3_falseProbs_01) = rocFromFile(new File(dir,"log_reg_length3_0.1fold.tsv"))
    printStats("length 3, 0.1-fold",length3_ROC_01,length3_precRec_01)
    val (length3Lda_ROC_01,length3Lda_precRec_01, length3Lda_trueProbs_01, length3Lda_falseProbs_01) = rocFromFile(new File(dir,"log_reg_lda_length3_0.1fold.tsv"))
    printStats("length 3 with lda, 0.1-fold",length3Lda_ROC_01,length3Lda_precRec_01)
    val aucs = List(10.0 -> getAUC(length3_ROC_01), 25.0 -> getAUC(length3_ROC_025), 50.0 -> getAUC(length3_ROC_2), 80.0 -> getAUC(length3_ROC_5), 90.0 -> getAUC(length3_ROC))
    val aucsLDA = List(10.0 -> getAUC(length3Lda_ROC_01), 25.0 -> getAUC(length3Lda_ROC_025), 50.0 -> getAUC(length3Lda_ROC_2), 80.0 -> getAUC(length3Lda_ROC_5), 90.0 -> getAUC(length3Lda_ROC))
    f = Figure()
    f.height = (f.width * 5)/9
    p = f.subplot(0)
    p += plot(aucs.map(_._1).toList, aucs.map(_._2).toList, name = "plain")
    p += plot(aucsLDA.map(_._1).toList, aucsLDA.map(_._2).toList, name = "lda")
    p.xlim = (10.0,90.0)
    p.ylim = (0.5,1.0)
    p.setXAxisDecimalTickUnits()
    p.setYAxisDecimalTickUnits()
    p.ylabel = "AUC"
    p.xlabel = "% used for training"
    p.title = "Change of AUC"
    p.legend = true
    f.saveas(new File(imgDir,"lda_vs_plain.png").getAbsolutePath, dpi = 300)
}



//compare topics
if(dir.list().exists(_.contains("lda300"))) {
    val (length3Lda300_ROC,length3Lda300_precRec, _, _) = rocFromFile(new File(dir,"log_reg_lda300_length3.tsv"))
    printStats("length 3 with lda300",length3Lda300_ROC,length3Lda300_precRec)
    val (length3Lda500_ROC,length3Lda500_precRec, _, _) = rocFromFile(new File(dir,"log_reg_lda500_length3.tsv"))
    printStats("length 3 with lda500",length3Lda500_ROC,length3Lda500_precRec)
    val (length3GPCA_ROC,length3GPCA_precRec, _, _) = rocFromFile(new File(dir,"log_reg_gpca_length3.tsv"))
    printStats("length 3 with gpca",length3GPCA_ROC,length3GPCA_precRec)
    f = Figure()
    f.height = f.width
    p = f.subplot(0)
    p += plot(length3Lda_ROC.map(_._1).toList, length3Lda_ROC.map(_._2).toList, name = "length3, 100 topics")
    p += plot(length3Lda300_ROC.map(_._1).toList, length3Lda300_ROC.map(_._2).toList, name = "length3, 300 topics")
    p += plot(length3Lda500_ROC.map(_._1).toList, length3Lda500_ROC.map(_._2).toList, name = "length3, 500 topics")
    p += plot(length3GPCA_ROC.map(_._1).toList, length3GPCA_ROC.map(_._2).toList, name = "gpca, 100")
    p += plot(List(0, 1), List(0, 1), name ="", colorcode = "black")
    p.xlim = (0.0,1.0)
    p.ylim = (0.0,1.0)
    p.setXAxisDecimalTickUnits()
    p.setYAxisDecimalTickUnits()
    p.xlabel = "false-positive rate"
    p.ylabel = "true-positive rate"
    p.title = "ROC"
    p.legend = true
    f.saveas(new File(imgDir,"lda_gpca.png").getAbsolutePath, dpi = 300)
}














