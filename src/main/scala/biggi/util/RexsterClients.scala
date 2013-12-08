package biggi.util

import scala.collection.parallel.mutable.ParArray
import com.tinkerpop.rexster.protocol.msg._
import scala.util.Random
import org.apache.commons.configuration.BaseConfiguration
import com.tinkerpop.rexster.client.{RexsterClient, RexsterClientFactory, RexsterClientTokens}
import java.util.UUID
import scala.Some
import java.util
import scala.collection.GenSeq
import scala.collection.JavaConversions._
import org.apache.commons.logging.LogFactory

/**
 * @author dirk
 *          Date: 11/11/13
 *          Time: 2:29 PM
 */
object RexsterClients {

    private final val LOG = LogFactory.getLog(getClass)

    type Session = Array[Byte]
    type RList[E] = util.ArrayList[E]

    case class GraphClient(client:RexsterClient,session:Session,idx:Int=0)

    def closeClients(graphClients: GenSeq[GraphClient]) {
        graphClients.foreach(c => {
            val sessionReq = new SessionRequestMessage
            sessionReq.Session = c.session
            //newSessionRequest.Session = new Array[Byte](16)
            //Random.nextBytes(newSessionRequest.Session)
            sessionReq.Request = new Array[Byte](16)
            Random.nextBytes(sessionReq.Request)
            sessionReq.Username = "rexster"
            sessionReq.Password = "rexster"
            sessionReq.metaSetKillSession(true)
            c.client.execute(sessionReq)

            c.client.close()
        })
    }

    def createGraphClients(hosts: Array[String]): ParArray[GraphClient] = {
        hosts.map(h => {
            createGraphClient(h)
        }).toSeq.zipWithIndex.map(e => GraphClient(e._1.client, e._1.session, e._2)).par.toParArray
    }


    def createGraphClient(h: String): GraphClient = {
        val Array(host, port, graph) = h.split(":", 3)
        val conf = new BaseConfiguration() {
            addProperty(RexsterClientTokens.CONFIG_HOSTNAME, host)
            addProperty(RexsterClientTokens.CONFIG_PORT, port)
            addProperty(RexsterClientTokens.CONFIG_TIMEOUT_CONNECTION_MS, 30000)
            addProperty(RexsterClientTokens.CONFIG_TIMEOUT_READ_MS, 60000)
            addProperty(RexsterClientTokens.CONFIG_TIMEOUT_WRITE_MS, 60000)
            addProperty(RexsterClientTokens.CONFIG_TRANSACTION, "true")
            //addProperty(RexsterClientTokens.CONFIG_SERIALIZER, 1)
        }

        val client = RexsterClientFactory.open(conf)
        val newSessionRequest = new SessionRequestMessage
        newSessionRequest.metaSetGraphName(graph)
        newSessionRequest.metaSetGraphObjName("g")
        //newSessionRequest.Session = new Array[Byte](16)
        //Random.nextBytes(newSessionRequest.Session)
        newSessionRequest.Request = new Array[Byte](16)
        Random.nextBytes(newSessionRequest.Request)
        newSessionRequest.Username = "rexster"
        newSessionRequest.Password = "rexster"

        val resp = client.execute(newSessionRequest)
        val session = resp.Session

        GraphClient(client, session)
    }

    def runScript[T <: Object](client: GraphClient,script:String, bindings:Map[String,_ <: AnyRef] = Map[String,AnyRef]()): Option[T] = {
        val query = new  ScriptRequestMessage
        query.Session = client.session
        query.metaSetInSession(true)
        query.metaSetIsolate(false)
        query.metaSetTransaction(false)
        query.setRequestAsUUID(UUID.randomUUID)
        query.LanguageName="groovy"
        if(!bindings.isEmpty) {
            query.Bindings = new RexProBindings()
            query.Bindings.putAll(bindings)
        }
        query.Script = script
        val resp = client.client.execute(query) match {
            case e: ErrorResponseMessage => LOG.error(e.ErrorMessage); None
            case null => None
            case r: ScriptResponseMessage => Some(r)
        }

        if(resp == None)
            None
        else if(resp.get.Results != null)
            resp.get.Results.get match {
                case t:T => Some(t)
                case _ => None
            }
        else
            None
    }

    def runScript[T <: Object](clients: GenSeq[GraphClient],script:String, bindings:Map[String,_ <: AnyRef]): GenSeq[Option[T]] = {
        clients.map(client => runScript(client,script,bindings))
    }

}
