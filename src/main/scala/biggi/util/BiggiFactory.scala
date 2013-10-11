package biggi.util

import akka.actor.ActorSystem
import java.io.File
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{Edge, Vertex}
import org.apache.commons.configuration.BaseConfiguration

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
        //titanConf.setProperty("storage.index.search.backend","lucene")
        //titanConf.setProperty("storage.index.search.directory",new File(dir,"searchindex").getAbsolutePath)
        titanConf
    }

    def getBigGraphConfiguration(dir:File) = {
        val titanConf = new BaseConfiguration()
        //titanConf.setProperty("storage.directory",new File(dir,"standard").getAbsolutePath)
        titanConf.setProperty("storage.backend","hbase")
        //titanConf.setProperty("storage.hostname","127.0.0.1")
        //titanConf.setProperty("storage.index.search.backend","lucene")
        //titanConf.setProperty("storage.index.search.directory",new File(dir,"searchindex").getAbsolutePath)
        titanConf
    }

    def initGraph(g:TitanGraph) {
        g.makeType().name("cui").dataType(classOf[String]).indexed(classOf[Vertex]).graphUnique().makePropertyKey()
        g.makeType().name("semtypes").dataType(classOf[String]).indexed(classOf[Vertex]).makePropertyKey()
        g.makeType().name("uttIds").dataType(classOf[String]).indexed(classOf[Edge]).makePropertyKey()
        g.makeType().name("count").dataType(classOf[java.lang.Integer]).indexed(classOf[Edge]).makePropertyKey()
        g.commit()
    }
}
