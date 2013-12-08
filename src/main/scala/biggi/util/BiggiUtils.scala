package biggi.util

import akka.actor.ActorSystem
import java.io.{FileOutputStream, FileInputStream, File}
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{Edge, Vertex}
import org.apache.commons.configuration.BaseConfiguration
import com.thinkaurelius.titan.core.TypeMaker.UniquenessConsistency
import com.esotericsoftware.kryo.io.{Output, Input}
import com.esotericsoftware.kryo.Kryo
import java.util

/**
 * @author dirk
 *          Date: 10/9/13
 *          Time: 2:29 PM
 */
object BiggiUtils {
    val actorSystem = ActorSystem("biggi")

    def getGraphConfiguration(dir:File) = {
        val titanConf = new BaseConfiguration()
        titanConf.setProperty("storage.directory",new File(dir,"standard").getAbsolutePath)
        titanConf.setProperty("storage.backend","berkeleyje")
        titanConf.setProperty("storage.index.lucene.backend","lucene")
        titanConf.setProperty("storage.index.lucene.directory",new File(dir,"lucene").getAbsolutePath)
        //titanConf.setProperty("autotype","none")
        //titanConf.setProperty("fast-property",true)

        titanConf
    }

    def getCuiToID(dir:File) = {
        val kryo = new Kryo()
        val input = new Input(new FileInputStream(new File(dir,"ui_id.bin")))
        val map = kryo.readObject(input, classOf[util.HashMap[String,java.lang.Long]])
        input.close()
        map
    }

    def saveCuiToID(dir:File,map:util.Map[String,java.lang.Long]) = {
        val kryo = new Kryo()
        val output = new Output(new FileOutputStream(new File(dir,"ui_id.bin")))
        kryo.writeObject(output, map)
        output.close()
    }

    def printGraphConfiguration(dir:File) = {
        val path = dir.getAbsolutePath
        s"""storage.backend=berkeleyje
          |storage.directory=$path/standard
          |storage.index.lucene.backend=lucene
          |storage.index.lucene.directory=$path/lucene
         """.stripMargin
    }

    def getBigGraphConfiguration(dir:File,name:String="") = {
        var n = name
        if(n == "")
            n = dir.getName
        val titanConf = new BaseConfiguration()
        //titanConf.setProperty("storage.directory",new File(dir,"standard").getAbsolutePath)
        titanConf.setProperty("storage.backend","hbase")
        titanConf.setProperty("storage.tablename",n)
        //titanConf.setProperty("storage.hostname","127.0.0.1")
        titanConf.setProperty("storage.index.lucene.backend","lucene")
        titanConf.setProperty("storage.index.lucene.directory",new File(dir,"lucene").getAbsolutePath)
        titanConf
    }

    def initGraph(g:TitanGraph) {
        g.makeKey(UI).dataType(classOf[String]).indexed(classOf[Vertex]).unique(UniquenessConsistency.LOCK).single().make()
        g.makeKey(TYPE).dataType(classOf[String]).indexed(classOf[Vertex]).indexed("lucene",classOf[Vertex]).make()
        g.makeKey(TEXT).dataType(classOf[String]).indexed("lucene",classOf[Vertex]).make()
        g.makeKey(DEPTH).dataType(classOf[Integer]).indexed(classOf[Vertex]).make()
        g.makeKey(TOTALDEPTH).dataType(classOf[Integer]).indexed(classOf[Vertex]).make()
        g.makeKey(SOURCE).dataType(classOf[String]).indexed(classOf[Edge]).make()
        g.makeKey(LABEL).dataType(classOf[String]).indexed(classOf[Edge]).indexed("lucene",classOf[Edge]).make()
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
    final val DEPTH = "depth"
    final val TOTALDEPTH = "totalDepth"
}
