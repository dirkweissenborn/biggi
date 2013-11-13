package biggi.util

import biggi.util.RexsterClients._
import scala.collection.JavaConversions._
import biggi.inference.ExtractInterestingPathsFromRexster

/**
 * @author dirk
 *          Date: 11/7/13
 *          Time: 5:12 PM
 */
object GremlinGraphs {

    def main(args:Array[String]) {
        val hosts = args(0).split(",")
        val clients = createGraphClients(hosts)
        clients.foreach(c => runScript[Object](c,"",Map("notAllowed" -> seqAsJavaList(ExtractInterestingPathsFromRexster.notAllowedEdges))))

        var query = ""
        while(query != "a") {
            println("Write your query:")
            query = readLine()
            if(query != "a")
                clients.map(c => runScript[RList[Object]](c, query).getOrElse(new RList[Object]()).foreach(o =>
                    println(hosts(c.idx)+"\t"+o)))
        }
        closeClients(clients)

        System.exit(0)
    }

}
