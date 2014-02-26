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

val map = scala.collection.mutable.Map[String,String]()

Source.fromFile(mrConsoFile).getLines().foreach(line => {
    val split = line.split("""\|""")
    val cui = split(0)
    val name = split(14)
    val lang = split(1)
    if(lang == "ENG" && !map.contains(cui))
        map += cui -> name
})


Source.fromFile(pairsFile).getLines().foreach(line => {
    val Array(cui1,cui2) = line.split(",",2)
    if(map.contains(cui1) && map.contains(cui2)) {
        val name1 = map(cui1)
        val name2 = map(cui2)

        println(name1+" & " + name2 + " \\\\")
    }
})









































































































































































































































































































































































































































































































































































































































































































































































