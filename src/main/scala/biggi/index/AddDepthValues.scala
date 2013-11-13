package biggi.index

import java.io.File
import biggi.util.BiggiFactory
import com.thinkaurelius.titan.core.TitanFactory
import scala.collection.JavaConversions._
import com.tinkerpop.blueprints.{Vertex, Direction}
import scala.collection.mutable
import com.thinkaurelius.titan.core.TypeMaker.UniquenessConsistency

/**
 * @author dirk
 *          Date: 11/5/13
 *          Time: 3:14 PM
 */
object AddDepthValues {

    def main(args:Array[String]) {
        val graphDir = new File(args(0))

        val conf = BiggiFactory.getGraphConfiguration(graphDir)
        conf.setProperty("storage.transactions","false")
        conf.setProperty("storage.batch-loading","true")
        conf.setProperty("autotype","none")

        val g = TitanFactory.open(conf)
        g.makeKey("depth").dataType(classOf[java.lang.Integer]).indexed(classOf[Vertex]).make()
        g.makeKey("totalDepth").dataType(classOf[java.lang.Integer]).indexed(classOf[Vertex]).make()
        var counter = 0

        //downwards: depth of v in hierarchy
        var stack = new mutable.Stack[(Vertex,Int)]()
        var path = List[(Vertex,Int)]()
        g.query().vertices().foreach(v => {
            val parents = v.getVertices(Direction.OUT,"isa")
            if(parents.isEmpty || (parents.size == 1 && parents.head == v))
                stack.push((v,1))
        })
        while(!stack.isEmpty) {
            val (v, depth) = stack.pop()
            
            path = (v,depth) :: path.dropWhile(_._2 >= depth)

            val oldDepth = v.getProperty[Integer]("depth") //is 0 for new property
            if(oldDepth == null || oldDepth < depth) {
                v.setProperty("depth",depth)

                stack.pushAll(v.getVertices(Direction.IN,"isa").filterNot(c => path.exists(_._1.equals(c))).map(c => (c,depth+1)))
                counter += 1
                if(counter % 10000 == 0) {
                    println(s"Depth of $counter concepts calculated/updated!")
                    g.commit()
                }
            }
        }

        counter = 0
        //upwards: total depth of path containing v
        stack = new mutable.Stack[(Vertex,Int)]()
        g.query().vertices().foreach(v => {
            val children = v.getVertices(Direction.IN,"isa")
            if(children.isEmpty || (children.size == 1 && children.head == v))
                stack.push((v,v.getProperty[Int]("depth")))
        })
        stack = stack.sortBy(-_._2)
        
        while(!stack.isEmpty) {
            val (v, totalDepth) = stack.pop()
            v.setProperty("totalDepth",totalDepth)

            stack.pushAll(
                v.getVertices(Direction.OUT,"isa").
                    filterNot(p => p.getPropertyKeys.contains("totalDepth")).
                    map(p => (p,totalDepth)))

            counter += 1
            if(counter % 10000 == 0) {
                println(s"Total depth of $counter concepts calculated!")
                g.commit()
            }
        }
        
        g.commit()
        g.shutdown()
        System.exit(0)
    }

}
