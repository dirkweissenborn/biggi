package biggi.index

import java.io.{FileWriter, PrintWriter, File}
import com.thinkaurelius.titan.core.{TitanLabel, TitanFactory}
import com.tinkerpop.blueprints.{Vertex, Graph, Direction}
import scala.collection.JavaConversions._
import com.tinkerpop.blueprints.Query.Compare
import org.apache.commons.logging.LogFactory
import biggi.util.BiggiUtils
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.collection.mutable.Map
import scala.concurrent.duration.Duration
import scala.io.Source
import biggi.model.deppath.DependencyPath

/**
 * @author dirk
 *          Date: 8/23/13
 *          Time: 10:33 AM
 */
object MergeGraphs {

    private final val LOG = LogFactory.getLog(getClass)

    private final var cuiIdMap = Map[String,java.lang.Long]()

    def main(args:Array[String]) {
        val inDir = new File(args(0))
        val outDir = new File(args(1))
        val ignoreDuplicates = args(2) == "true"
        val _override = if(args.size > 3) args(3) == "override" else false
        val allowedCuis = if(args.size > 4)
                Source.fromFile(args(4)).getLines().map(_.trim).toSet
            else Set[String]()
        val allowedEdges = if(args.size > 5)
            Source.fromFile(args(5)).getLines().map(e => DependencyPath.removeAttributes(e.trim)).toSet
        else Set[String]()

        def allowCui(cui:String) = allowedCuis.isEmpty || allowedCuis.contains(cui)
        def allowEdge(label:String) = allowedEdges.isEmpty || allowedEdges.contains(DependencyPath.removeAttributes(label))

        val newGraph = outDir.mkdirs() || _override || outDir.list().isEmpty

        if(_override){
            println("Overriding output directory!")
            def deleteDir(dir:File) {
                dir.listFiles().foreach(f => {
                    if (f.isDirectory) {
                        deleteDir(f)
                        f.delete()
                    }
                    else
                        f.delete()
                })
            }
            deleteDir(outDir)
        }

        val titanConf = BiggiUtils.getGraphConfiguration(outDir)
        titanConf.setProperty("storage.buffer-size","2048")
        titanConf.setProperty("ids.block-size","100000")
        titanConf.setProperty("storage.batch-loading",true)
        titanConf.setProperty("storage.cache-percentage",40)

        val bigGraph = TitanFactory.open(titanConf)
        if(newGraph) {
            LOG.info("New graph created!")
            BiggiUtils.initGraph(bigGraph)
        } else {
            cuiIdMap = BiggiUtils.getCuiToID(outDir)
            LOG.info("Loaded graph with "+cuiIdMap.size+" vertices!")
        }

        { //write configuration
            val pw = new PrintWriter(new FileWriter(new File(outDir,"graph.config")))
            pw.println(BiggiUtils.printGraphConfiguration(outDir))
            pw.close()
        }

        LOG.info("Merging into: "+ outDir.getAbsolutePath)

        val files = inDir.listFiles().iterator

        def getGraphVertices(indexDir:File) =  {
            try {
                val smallConf = BiggiUtils.getGraphConfiguration(indexDir)
                smallConf.setProperty("storage.transactions","false")
                smallConf.setProperty("storage.cache-percentage",1)

                //smallConf.setProperty("storage.read-only","true")
                val smallGraph = TitanFactory.open(smallConf)

                if(indexDir.list().exists(_.endsWith("ui_id.bin"))) {
                    val cuiIdMap = BiggiUtils.getCuiToID(indexDir)
                    (smallGraph,cuiIdMap.iterator.map(id => (smallGraph.getVertex(id._2),id._1)),indexDir)
                }
                else
                    (smallGraph,smallGraph.getVertices.iterator().map(v => (v,v.getProperty[String](BiggiUtils.UI))),indexDir)
            }
            catch {
                case t:Throwable => LOG.error(t.getMessage);(null,null,indexDir)
            }
        }

        //var nextGraphVertices = getGraphVertices(files.next())
        while(files.hasNext) {//nextGraphVertices != null) {
            try {
                var (smallGraph,vertices,indexDir) = getGraphVertices(files.next())//Await.result(nextGraphVertices,Duration.Inf)
                /*if(files.hasNext)
                    nextGraphVertices = getGraphVertices(files.next())
                else
                    nextGraphVertices = null*/

                var counter = 0

                LOG.info("Merging from: "+ indexDir.getAbsolutePath)
                if(vertices != null) {
                    vertices.foreach{ case (fromSmall,fromCui) =>
                        if(!fromCui.contains(".") && allowCui(fromCui)) {
                            val fromId = cuiIdMap.get(fromCui)

                            val from =
                                if(fromId.isDefined)
                                    bigGraph.getVertex(fromId.get)
                                else {
                                    val v = addVertex(newGraph, bigGraph, fromCui, fromSmall)
                                    cuiIdMap += fromCui -> v.getId.asInstanceOf[java.lang.Long]
                                    v
                                }

                            var smallEdges = fromSmall.getEdges(Direction.OUT).iterator().toList.groupBy(e => {
                                if(!e.getPropertyKeys.contains(BiggiUtils.LABEL))
                                    (e.getLabel,e.getVertex(Direction.IN).getProperty[String](BiggiUtils.UI))
                                else
                                    (e.getProperty[String](BiggiUtils.LABEL),e.getVertex(Direction.IN).getProperty[String](BiggiUtils.UI))
                            } )

                            if(!ignoreDuplicates)
                                from.getEdges(Direction.OUT).foreach(bigEdge => {
                                    val key = (bigEdge.getProperty[String](BiggiUtils.LABEL), bigEdge.getVertex(Direction.IN).getProperty[String](BiggiUtils.UI))
                                    smallEdges.get(key) match {
                                        case Some(edges) =>
                                            bigEdge.setProperty(BiggiUtils.SOURCE,(bigEdge.getProperty[String](BiggiUtils.SOURCE).split(",") ++ edges.flatMap(_.getProperty[String](BiggiUtils.SOURCE).split(","))).toSet.mkString(","))
                                            smallEdges -= key
                                        case None =>
                                    }
                                } )

                            smallEdges.withFilter(edge => allowCui(edge._1._2) &&
                                                          allowEdge(edge._1._1)).
                                foreach {
                                    case ((label,toCui),smallEdges) => {
                                        val toId = cuiIdMap.get(toCui)

                                        val to =
                                            if(toId.isDefined)
                                                bigGraph.getVertex(toId.get)
                                            else {
                                                val v = addVertex(newGraph, bigGraph, toCui, smallEdges.head.getVertex(Direction.IN))
                                                cuiIdMap += toCui -> v.getId.asInstanceOf[java.lang.Long]
                                                v
                                            }

                                        /*if(!indexedEdges.contains(label)) {
                                            bigGraph.makeLabel(label).manyToMany().make()
                                            indexedEdges += label
                                        }*/
                                        val e = bigGraph.addEdge(null,from,to,BiggiUtils.EDGE)
                                        smallEdges.foreach(smallEdge => smallEdge.getPropertyKeys.withFilter(k => k != BiggiUtils.SOURCE && k != BiggiUtils.LABEL).foreach(key => e.setProperty(key,smallEdge.getProperty(key))))
                                        e.setProperty(BiggiUtils.SOURCE, smallEdges.flatMap(_.getProperty[String](BiggiUtils.SOURCE).split(",")).toSet.mkString(","))
                                        e.setProperty(BiggiUtils.LABEL, label)
                                    }
                                }

                            counter += 1
                            if(counter % 10000 == 0) {
                                LOG.info(counter+" vertices of "+ indexDir.getName+" processed")
                                bigGraph.commit()
                            }
                        }
                    }
                    smallGraph.shutdown()
                    bigGraph.commit()

                    smallGraph = null
                    vertices = null
                    System.gc()
                }
            } catch {
                case t:Throwable => LOG.error(t.printStackTrace())
            }
        }
        bigGraph.shutdown()
        BiggiUtils.saveCuiToID(outDir,cuiIdMap)
        LOG.info("DONE!")
        System.exit(0)
    }


    def addVertex(newGraph: Boolean, graph: Graph, cui: String, smallVertex: Vertex): Vertex = {
        val it1 = if (!newGraph) graph.query.has(BiggiUtils.UI, Compare.EQUAL, cui).limit(1).vertices() else null
        if (!newGraph && !it1.isEmpty)
            it1.head
        else {
            val v = graph.addVertex(null)
            smallVertex.getPropertyKeys.foreach(propKey => {
                v.setProperty(propKey, smallVertex.getProperty(propKey))
            })
            v
        }
    }
}
