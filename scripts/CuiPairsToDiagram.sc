import breeze.plot.ExportGraphics
import java.awt.{Color, Font}
import java.io.{FileWriter, PrintWriter, File}
import java.util
import org.jfree.chart.plot.{PlotOrientation, PiePlot}
import org.jfree.chart.renderer.category.{StandardBarPainter, BarRenderer}
import org.jfree.chart.{ChartUtilities, ChartPanel, ChartFactory, JFreeChart}
import org.jfree.data.category.{CategoryToPieDataset, DefaultCategoryDataset}
import org.jfree.data.general.{DatasetChangeListener, DatasetGroup, PieDataset}
import org.jfree.ui.{RefineryUtilities, ApplicationFrame}
import org.jfree.util.TableOrder
import scala.collection.mutable
import scala.io.Source

/**
 * Created by dirkw on 1/29/14.
 */
val mrConsoFile = new File("/ssd/data/umls/MRCONSO.RRF")
val pairsFile = new File("/ssd/data/pos_drug_target.tsv")
def normalizeName(name:String) = name.trim.toLowerCase.replaceAll("-"," ")
val map = scala.collection.mutable.Map[String,String]()
var meshTreeToName = Map[String,String]()
val meshTreeNums = Source.fromFile("/ssd/data/mtrees2014.bin").getLines().map(line => {
    val Array(name,treenum) = line.split(";")
    if(!treenum.contains("."))
        meshTreeToName += treenum -> name
    (name,treenum)
}).toSeq.groupBy(_._1).mapValues(_.map(_._2))

Source.fromFile(mrConsoFile).getLines().foreach(line => {
    val split = line.split("""\|""")
    val cui = split(0)
    val source = split(11)
    val name = split(14)
    val lang = split(1)
    if(lang == "ENG" && !map.contains(cui) && source == "MSH" && meshTreeNums.contains(name))
        map += cui -> name
})
var firstParents = mutable.Map[String,Double]()
var secondParents = mutable.Map[String,Double]()

Source.fromFile(pairsFile).getLines().foreach(line => {
    val Array(cui1,cui2) = line.split(",",2)
    if(map.contains(cui1) && map.contains(cui2)) {
        val name1 = map(cui1)
        val name2 = map(cui2)

        val treenums1 = meshTreeNums(name1)
        treenums1.foreach(treenum => {
            val key = meshTreeToName(treenum.takeWhile(_ != '.')).replaceAll(" Diseases?","")
            if(!firstParents.contains(key))
                firstParents += key -> 1.0 / treenums1.size
            else
                firstParents(key) += 1.0 / treenums1.size
        })
        val treenums2 = meshTreeNums(name2)
        treenums2.foreach(treenum => {
            val key = meshTreeToName(treenum.takeWhile(_ != '.')).replaceAll(" Diseases?","")
            if(!secondParents.contains(key))
                secondParents += key -> 1.0 / treenums2.size
            else
                secondParents(key) += 1.0 / treenums2.size
        })
    }
})
firstParents
secondParents
val firstDataset = new DefaultCategoryDataset()
firstParents.toSeq.sortBy(-_._2).foreach{ case (key,value) =>
    firstDataset.addValue(value,"count",key)
}
val secondDataset = new DefaultCategoryDataset()
secondParents.toSeq.sortBy(-_._2).foreach{ case (key,value) =>
    secondDataset.addValue(value,"count",key)
}
val chart1 = ChartFactory.createBarChart("","","",firstDataset,PlotOrientation.HORIZONTAL,false,false,false)
val chart2 = ChartFactory.createBarChart("","","",secondDataset,PlotOrientation.HORIZONTAL,false,false,false)


val scale = 4
val legenFont = new Font("legend",Font.PLAIN, 7*scale)

chart1.setBackgroundImageAlpha(0.0f)
chart1.getPlot.setBackgroundAlpha(0.0f)
chart1.getCategoryPlot.getRenderer.asInstanceOf[BarRenderer].setBarPainter(new StandardBarPainter())
chart1.getCategoryPlot.getRenderer.setSeriesPaint(0,Color.BLUE)
chart1.getCategoryPlot.getDomainAxis.setTickLabelFont(legenFont)
chart1.getCategoryPlot.getRangeAxis.setTickLabelFont(legenFont)

chart2.setBackgroundImageAlpha(0.0f)
chart2.getPlot.setBackgroundAlpha(0.0f)
chart2.getCategoryPlot.getRenderer.asInstanceOf[BarRenderer].setBarPainter(new StandardBarPainter())
chart2.getCategoryPlot.getRenderer.setSeriesPaint(0,Color.BLUE)
chart2.getCategoryPlot.getDomainAxis.setTickLabelFont(legenFont)
chart2.getCategoryPlot.getRangeAxis.setTickLabelFont(legenFont)

val chartPanel1 = new ChartPanel(chart1)
chartPanel1.setPreferredSize(new java.awt.Dimension(300, 300))
val frame1 = new ApplicationFrame("Plot")
frame1.setContentPane(chartPanel1)
frame1.pack()
RefineryUtilities.centerFrameOnScreen(frame1)
frame1.setVisible(true)
val chartPanel2 = new ChartPanel(chart2)
chartPanel2.setPreferredSize(new java.awt.Dimension(300, 300))
val frame2 = new ApplicationFrame("Plot")
frame2.setContentPane(chartPanel2)
frame2.pack()
RefineryUtilities.centerFrameOnScreen(frame2)
frame2.setVisible(true)

ChartUtilities.saveChartAsPNG(new File("/home/bioinf/dirkw/Dropbox/Diplom/paper/images/has_target/drugs.png"), chart1, 300*scale, firstParents.size*20*scale)
ChartUtilities.saveChartAsPNG(new File("/home/bioinf/dirkw/Dropbox/Diplom/paper/images/has_target/targets.png"), chart2, 300*scale, secondParents.size*20*scale)
/*
val output1 = "/tmp/first.tsv"
val output2 = "/tmp/second.tsv"

var pw = new PrintWriter(new FileWriter(output1))
firstParents.toSeq.sortBy(-_._2).foreach{ case (key,value) =>
    pw.println(key+"\t"+value)
}
pw.close()

pw = new PrintWriter(new FileWriter(output2))
secondParents.toSeq.sortBy(-_._2).foreach{ case (key,value) =>
    pw.println(key+"\t"+value)
}
pw.close()


data=read.delim("/tmp/first.tsv",header=FALSE)
png(filename="/home/bioinf/dirkw/Dropbox/Diplom/paper/images/may_treat/drugs.png",res=300, width=2000, height=1500)
par(mar=c(5,20,4,2))
par(las=2)
barplot(revdata$V2,names.arg=revdata$V1,horiz=TRUE, cex.names=0.8, col="skyblue3",xlim=c(0,250))
dev.off()

png(filename="/home/bioinf/dirkw/Dropbox/Diplom/paper/images/may_treat/diseases.png",res=300, width=1500, height=1500)
par(mar=c(5,10,4,1))
par(las=2)

*/


