package biggi.util

import akka.actor.ActorSystem
import java.io.File
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{Edge, Vertex}
import org.apache.commons.configuration.BaseConfiguration
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel
import com.thinkaurelius.titan.core.TypeMaker.UniquenessConsistency

/**
 * @author dirk
 *          Date: 10/9/13
 *          Time: 2:29 PM
 */
object BiggiFactory {
    val actorSystem = ActorSystem("biggi")

    def getGraphConfiguration(dir:File) = {
        val titanConf = new BaseConfiguration()
        titanConf.setProperty("storage.directory",new File(dir,"standard").getAbsolutePath)
        titanConf.setProperty("storage.backend","berkeleyje")
        titanConf.setProperty("storage.index.lucene.backend","lucene")
        titanConf.setProperty("storage.index.lucene.directory",new File(dir,"lucene").getAbsolutePath)
        //titanConf.setProperty("autotype","none")
        titanConf.setProperty("fast-property",true)
        titanConf
    }

    def printGraphConfiguration(dir:File) = {
        val path = dir.getAbsolutePath
        s""""storage.backend=berkeleyje
          |storage.directory=$path/standard
          |storage.index.lucene.backend=lucene
          |storage.index.lucene.directory=$path/lucene
         """.stripMargin
    }

    def getBigGraphConfiguration(dir:File) = {
        val titanConf = new BaseConfiguration()
        //titanConf.setProperty("storage.directory",new File(dir,"standard").getAbsolutePath)
        titanConf.setProperty("storage.backend","hbase")
        //titanConf.setProperty("storage.hostname","127.0.0.1")
        titanConf.setProperty("storage.index.lucene.backend","lucene")
        titanConf.setProperty("storage.index.lucene.directory",new File(dir,"lucene").getAbsolutePath)
        titanConf
    }

    def initGraph(g:TitanGraph) {
        g.makeKey(UI).dataType(classOf[String]).indexed(classOf[Vertex]).unique(UniquenessConsistency.LOCK).single().make()
        g.makeKey(TYPE).dataType(classOf[String]).indexed(classOf[Vertex]).indexed("lucene",classOf[Vertex]).make()
        g.makeKey(TEXT).dataType(classOf[String]).indexed("lucene",classOf[Vertex]).make()
        g.makeKey(SOURCE).dataType(classOf[String]).indexed(classOf[Edge]).make()
        g.makeKey(LABEL).dataType(classOf[String]).indexed(classOf[Edge]).make()
        g.makeKey(WEIGHT).dataType(classOf[java.lang.Double]).indexed(classOf[Edge]).make()
        g.makeLabel(EDGE).manyToMany().make()
        g.commit()
    }

    final val UI = "ui"
    final val TYPE = "type"
    final val TEXT = "text"
    final val SOURCE = "source"
    final val WEIGHT = "weight"
    final val LABEL = "plabel"
    final val EDGE = "co-occurs"
}
