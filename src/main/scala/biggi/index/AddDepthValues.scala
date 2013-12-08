package biggi.index

import java.io.File
import biggi.util.BiggiUtils
import com.thinkaurelius.titan.core.TitanFactory
import scala.collection.JavaConversions._
import com.tinkerpop.blueprints.{Edge, Vertex, Direction}
import scala.collection.mutable
import com.tinkerpop.gremlin.scala._
/**
 * @author dirk
 *          Date: 11/5/13
 *          Time: 3:14 PM
 */
object AddDepthValues {

    def main(args:Array[String]) {
        val graphDir = new File(args(0))

        val conf = BiggiUtils.getGraphConfiguration(graphDir)
        conf.setProperty("storage.batch-loading","true")
        conf.setProperty("storage.cache-percentage","10")
        conf.setProperty("autotype","none")

        val g = TitanFactory.open(conf)
        if(!g.getIndexedKeys(classOf[Vertex]).containsAll(List("depth","totalDepth"))) {
            g.makeKey("depth").dataType(classOf[java.lang.Integer]).indexed(classOf[Vertex]).make()
            g.makeKey("totalDepth").dataType(classOf[java.lang.Integer]).indexed(classOf[Vertex]).make()
        }
        var counter = 0

        //downwards: depth of v in hierarchy
        val downStack = new mutable.Stack[(Vertex,Int)]()
        var upStack = new mutable.Stack[(Vertex,Int)]()

        val cuiIdMap = BiggiUtils.getCuiToID(graphDir)


        var path = List[(Vertex,Int)]()
        cuiIdMap.foreach { case (_,id) =>
            val v = g.getVertex(id)
            val parents = v.outE.filter((e:Edge) => {val label = e.getProperty(BiggiUtils.LABEL); label == "CHD" || label == "RN"}).inV.toScalaList()
            if(!parents.exists(_ != v))
                downStack.push((v,1))

            val children = v.inE.filter((e:Edge) => {val label = e.getProperty(BiggiUtils.LABEL); label == "CHD" || label == "RN"}).outV.toScalaList()
            if(!children.exists(_ != v))
                upStack.push((v,0))

            counter += 1
            if(counter % 10000 == 0) {
                println(s"$counter concepts pre-processed!")
            }
        }

        counter = 0

        while(!downStack.isEmpty) {
            val (v, depth) = downStack.pop()
            path = (v,depth) :: path.dropWhile(_._2 >= depth)
            val oldDepth = v.getProperty[Integer]("depth") //is 0 for new property

            if(oldDepth == null || oldDepth > depth) {
                v.setProperty("depth",depth)
                val children = v.query().direction(Direction.IN).has(BiggiUtils.LABEL, "CHD").vertices()
                downStack.pushAll(children.filterNot(c => path.exists(_._1.equals(c))).map(c => (c,depth+1)))
                counter += 1
                if(counter % 10000 == 0) {
                    println(s"Depth of $counter concepts calculated/updated!")
                    g.commit()
                }
            }
        }

        def handleUpperCycle(vertex:Vertex) {
            path = List[(Vertex,Int)]()
            val cycleStack = new mutable.Stack[(Vertex,Int)]()
            cycleStack.push((vertex,1))
            while(!cycleStack.isEmpty) {
                val (v, idx) = cycleStack.pop()
                path = (v,idx) :: path.dropWhile(_._2 >= idx)
                val oldDepth = v.getProperty[Integer]("depth") //is 0 for new property

                if(oldDepth == null) {
                    val newVertices = v.outE.filter((e:Edge) => {val label = e.getProperty[String](BiggiUtils.LABEL); label == "CHD" || label == "RN"}).inV.
                        map((v:Vertex) => (v,idx+1)).toScalaList()
                    val filteredVertices = newVertices.filterNot(c => path.exists(_._1.equals(c._1)))
                    val parentDepths = filteredVertices.map(_._1.getProperty[Integer]("depth")).filter(_ != null)

                    cycleStack.pushAll(filteredVertices.filter(_._1.getProperty[Integer]("depth") == null))

                    if(!parentDepths.isEmpty) {
                        var depth = parentDepths.max + 1
                        path.foreach(v => {v._1.setProperty("depth",depth);depth += 1})
                    } else if(filteredVertices.isEmpty) {
                        var take = true
                        val cycle = path.takeWhile(v => {
                            val temp = take
                            take = newVertices.exists(_._1.equals(v._1))
                            temp
                        })
                        cycle.foreach(v => v._1.setProperty("depth",1))
                        val rest = path.drop(cycle.size)
                        var depth = 2
                        rest.foreach(v => {
                            if(v._1.getProperty[Integer]("depth") == null || v._1.getProperty[Integer]("depth") < depth)
                                v._1.setProperty("depth",depth);depth += 1 })
                    }
                }
            }
        }

        counter = 0
        //upwards: total depth of path containing v
        println("Handling upper cycles...")
        cuiIdMap.values().foreach(id => {
            val v = g.getVertex(id)
            if(v.getProperty("depth") == null)
                handleUpperCycle(v)
            assert(v.getProperty("depth") != null)
            counter += 1
            if(counter % 10000 == 0) {
                println(s"Depth of $counter concepts checked!")
                g.commit()
            }
        })
        upStack = upStack.flatMap(v => {
            try {
                val depth = v._1.getProperty[Integer]("depth").toInt
                v._1.setProperty("totalDepth",depth)
                v._1.query().direction(Direction.OUT).has(BiggiUtils.LABEL,"CHD").vertices().map(c => (c, depth))
            } catch {
                case t:Throwable => println(v._1.getProperty[String](BiggiUtils.UI)); throw t
            }
        }).sortBy(-_._2)
        
        counter = 0
        while(!upStack.isEmpty) {
            var (v, totalDepth) = upStack.pop()
            val depth = v.getProperty[Integer]("depth")
            if(depth <= totalDepth)
                totalDepth= depth + 1

            v.setProperty("totalDepth",totalDepth)

            upStack.pushAll(
                v.outE.filter((e:Edge) => {val label = e.getProperty[String](BiggiUtils.LABEL); label == "CHD" || label == "RN"}).inV().
                    filter((v:Vertex) => v.getPropertyKeys.contains("totalDepth") && v.getProperty[Integer]("totalDepth") > totalDepth).
                    map((v:Vertex) => (v,totalDepth)).toScalaList())
            counter += 1
            if(counter % 10000 == 0) {
                println(s"Total depth of $counter concepts calculated!")
                g.commit()
            }
        }

        def handleLowerCycle(vertex:Vertex) {
            path = List[(Vertex,Int)]()
            val cycleStack = new mutable.Stack[(Vertex,Int)]()
            cycleStack.push((vertex,1))
            while(!cycleStack.isEmpty) {
                val (v, idx) = cycleStack.pop()
                path = (v,idx) :: path.dropWhile(_._2 >= idx)
                val oldDepth = v.getProperty[Integer]("totalDepth") //is 0 for new property

                if(oldDepth == null) {
                    val newVertices = v.inE.filter((e:Edge) => {val label = e.getProperty[String](BiggiUtils.LABEL); label == "CHD" || label == "RN"}).outV().
                        map((v:Vertex) => (v,idx+1)).toScalaList()
                    val filteredVertices = newVertices.filterNot(c => path.exists(_._1.equals(c._1)))
                    val childrenDepths = filteredVertices.map(_._1.getProperty[Integer]("totalDepth")).filter(_ != null)

                    cycleStack.pushAll(filteredVertices.filter(_._1.getProperty[Integer]("totalDepth") == null))

                    if(!childrenDepths.isEmpty) {
                        val td = childrenDepths.max
                        path.foreach(_._1.setProperty("totalDepth",td))
                    } else if(filteredVertices.isEmpty) {
                        var take = true
                        val cycle = path.takeWhile(v => {
                            val temp = take
                            take = newVertices.exists(_._1.equals(v._1))
                            temp
                        })

                        val td: Integer = cycle.maxBy(_._1.getProperty[Integer]("depth"))._1.getProperty[Integer]("depth")
                        path.foreach(v => {
                            if(v._1.getProperty[Integer]("totalDepth") == null || v._1.getProperty[Integer]("totalDepth") < td)
                                v._1.setProperty("totalDepth",td)})
                    }
                }
            }
        }
        counter = 0
        println("Handling lower cycles...")
        cuiIdMap.values().foreach(id => {
            val v = g.getVertex(id)
            if(v.getProperty("totalDepth") == null)
                handleLowerCycle(v)

            assert(v.getProperty("depth") != null)
            counter += 1
            if(counter % 10000 == 0) {
                println(s"Total depth of $counter concepts checked!")
                g.commit()
            }
        })
        
        g.commit()
        g.shutdown()
        System.exit(0)
    }

}
