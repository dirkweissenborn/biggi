package biggi.inference

import java.io.File
import scala.collection.JavaConversions._
import biggi.util.BiggiUtils
import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.{Graph, Direction, Edge, Vertex}
import org.apache.commons.logging.LogFactory
import scala.collection.GenSeq
import scala.collection.mutable
import java.util

/**
 * @author dirk
 *          Date: 11/18/13
 *          Time: 12:17 PM
 */
object PrecalculateParameters {

    private val LOG = LogFactory.getLog(getClass)

    def main(args:Array[String]) {
        val graphsDir = new File(args(0))

        val listFiles = graphsDir.listFiles()
        val graphs = listFiles.map(file => {
            val conf = BiggiUtils.getGraphConfiguration(file)
            conf.setProperty("storage.transactions","false")
            //conf.setProperty("storage.batch-loading","true")
            conf.setProperty("autotype","none")
            conf.setProperty("storage.cache-percentage",60/listFiles.size)

            val g = TitanFactory.open(conf)

            if(!g.getIndexedKeys(classOf[Vertex]).contains("degree"))
                g.makeKey("degree").dataType(classOf[java.lang.Integer]).indexed(classOf[Vertex]).make()
            if(!g.getIndexedKeys(classOf[Vertex]).contains("depth"))
                g.makeKey("depth").dataType(classOf[java.lang.Integer]).indexed(classOf[Vertex]).make()
            if(!g.getIndexedKeys(classOf[Vertex]).contains("totalDepth"))
                g.makeKey("totalDepth").dataType(classOf[java.lang.Integer]).indexed(classOf[Vertex]).make()
            if(!g.getIndexedKeys(classOf[Edge]).contains("f_degree")) {
                g.makeKey("f_degree").dataType(classOf[java.lang.Integer]).indexed(classOf[Edge]).make()
                g.makeKey("t_degree").dataType(classOf[java.lang.Integer]).indexed(classOf[Edge]).make()
            }
            g.commit()

            (g, BiggiUtils.getCuiToID(file))
        }).toSeq.par

        val cuis = graphs.foldLeft(Set[String]())(_ ++ _._2.keySet())
        LOG.info("Total number of vertices: "+cuis.size)
        var counter = 0
        var avgDegree = 0.0

        cuis.foreach(cui => {
            val groupedEdges = getGroupedEdges(graphs, cui)

            avgDegree += addParameters(groupedEdges, graphs, cui)

            counter += 1

            if(counter % 100 == 0) {
                LOG.info(s"$counter vertices processed")
                LOG.info(s"Avg. degree: "+ (avgDegree/counter))
                graphs.foreach(g => {
                    try {
                        g._1.commit()
                    } catch {
                        case t:Throwable => LOG.error(t.printStackTrace())
                    }
                })
            }
        })
        graphs.foreach(_._1.commit())
        graphs.foreach(_._1.shutdown())
        System.exit(0)
    }


    def getGroupedEdges(graphs: GenSeq[(Graph, util.HashMap[String, java.lang.Long])], cui: String): Map[CEdge, Seq[Edge]] = {
        val edges = graphs.flatMap {
            case (g, cuiIdMap) =>
                cuiIdMap.get(cui) match {
                    case null => List[Edge]()
                    case id => g.getVertex(id).getEdges(Direction.BOTH).toList
                }
        }.seq

        //group duplicate edges
        val groupedEdges = edges.groupBy(edge => {
            CEdge(edge.getVertex(Direction.OUT).getProperty[String](BiggiUtils.UI),
                edge.getProperty[String](BiggiUtils.LABEL),
                edge.getVertex(Direction.IN).getProperty[String](BiggiUtils.UI))
        })
        groupedEdges
    }

    def addParameters(groupedEdges: Map[CEdge, Seq[Edge]], graphs: GenSeq[(Graph, util.HashMap[String, java.lang.Long])], cui: String):Int = {
        val degree = groupedEdges.size

        var depth,totalDepth = 0
        //set degree
        graphs.foreach {
            case (g, cuiIdMap) =>
                cuiIdMap.get(cui) match {
                    case null =>
                    case id =>
                        val v = g.getVertex(id)
                        v.setProperty("degree", degree)
                        v.getProperty[Integer]("depth") match {
                            case null => //...
                            case d:Integer => depth = d
                        }
                        v.getProperty[Integer]("totalDepth") match {
                            case null => //...
                            case td:Integer => totalDepth = td
                        }
                }
        }

        //set depth
        graphs.foreach {
            case (g, cuiIdMap) =>
                cuiIdMap.get(cui) match {
                    case null => // ...
                    case id => val v = g.getVertex(id); v.setProperty("depth", depth); v.setProperty("totalDepth", totalDepth)
                }
        }

        //(label,isOutEdge) -> edges
        val labelGroupedEdges = groupedEdges.groupBy(e => (e._1.label, e._1.from == cui))

        //set specific degree
        labelGroupedEdges.foreach {
            case ((label, out), edges) =>
                try {
                    val s_degree = edges.size
                    val propKey = {
                        if (out) "f_degree" else "t_degree"
                    }
                    edges.foreach(_._2.foreach(edge => edge.setProperty(propKey, s_degree)))
                }
                catch {
                    case t: Throwable => LOG.error(t.printStackTrace())
                }
            }
        degree
    }
}
