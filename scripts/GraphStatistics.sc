import biggi.util.BiggiUtils
import com.thinkaurelius.titan.core.TitanFactory
import java.io.File
import scala.collection.JavaConversions._
import com.tinkerpop.blueprints.Direction

/**
 * Created by dirkw on 2/21/14.
 */

val conf = BiggiUtils.getGraphConfiguration(new File("/ssd/data/umlsGraph"))
conf.setProperty("storage.transactions","false")
val g = TitanFactory.open(conf)






var ctr = 0
var edgeCtr = 0
g.getVertices.foreach(v => {
    ctr += 1

    edgeCtr += v.getVertices(Direction.OUT).size
})

println("nr of vertices: "+ctr)
println("nr of edges: "+edgeCtr)

g.shutdown()