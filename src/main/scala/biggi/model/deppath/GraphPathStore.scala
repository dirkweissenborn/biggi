package biggi.model.deppath

import java.io._
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import java.util
import com.sun.org.apache.commons.logging.LogFactory

/**
 * @author dirk
 *          Date: 12/13/13
 *          Time: 12:56 PM
 */
class GraphPathStore {
    private val LOG = LogFactory.getLog(getClass)

    private val kryo = new Kryo

    private var pairToEdges = new java.util.HashMap[String,java.util.HashMap[String,Int]]()
    private var paths = ArrayBuffer[ArrayBuffer[String]]()

    def addPath(path:ArrayBuffer[String]) {
        if(path.size > 1 && path.sliding(2).forall(l => containsPair(l(0),l(1))))
            paths.+=:(path)
        else
            LOG.error("Path did contain unknown edges (pairs of vertices)!")
    }

    def addPairFromVector(p1:String, p2:String, edges:Vector[String]) {
        if(pairToEdges.containsKey(s"$p1-$p2"))
            pairToEdges(p1+"-"+p2) ++= edges.groupBy(s => s).map(s => s._1 -> s._2.size)
        else if(pairToEdges.containsKey(s"$p2-$p1"))
            pairToEdges(p2+"-"+p1) ++= edges.groupBy(s => s).map(s => (s._1+"^-1") -> s._2.size)
    }

    def addPair(p1:String, p2:String, edges:Map[String,Int]) {
        if(pairToEdges.containsKey(s"$p1-$p2"))
            pairToEdges(p1+"-"+p2) ++= edges
        else if(pairToEdges.containsKey(s"$p2-$p1"))
            pairToEdges(p2+"-"+p1) ++= edges
        else {
            pairToEdges.put(p1+"-"+p2, new util.HashMap[String,Int]())
            pairToEdges(p1+"-"+p2).putAll(edges)
        }
    }

    def containsPair(p1:String, p2:String) = {
        pairToEdges.containsKey(s"$p1-$p2") || pairToEdges.containsKey(s"$p2-$p1")
    }

    def getEdges(p1:String, p2:String) = {
        pairToEdges.getOrElse(s"$p1-$p2",pairToEdges.getOrElse(s"$p2-$p1",new util.HashMap[String,Int]()).foldLeft(new util.HashMap[String,Int]())((acc,entry) => {
            if(entry._1.endsWith("^-1"))
                acc.put(entry._1.substring(0,entry._1.length-3), entry._2)
            else
                acc.put(entry._1+"^-1", entry._2)
            acc
        }))
    }

    def getPaths = paths

    def serialize(outputStream:OutputStream) {
        val out = new Output(outputStream)
        kryo.writeObject(out, pairToEdges)
        kryo.writeObject(out, paths)
        out.close()
    }

    def deserialize(inputStream:InputStream) {
        val in = new Input(inputStream)
        pairToEdges = kryo.readObject(in, pairToEdges.getClass)
        paths = kryo.readObject(in, paths.getClass)
        in.close()
    }
}

object GraphPathStore {
    def fromFile(file:File) = {
        val store = new GraphPathStore
        store.deserialize(new FileInputStream(file))
        store
    }
}
