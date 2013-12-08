package biggi.inference

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.parallel.mutable.ParArray
import biggi.util.RexsterClients._
import java.{lang}
import java.io.File
import biggi.model.CountStore
import scala.Some
import biggi.util.RexsterClients.GraphClient
import biggi.util.BiggiUtils
import scala.concurrent.Future


/**
 * @author dirk
 *          Date: 10/9/13
 *          Time: 3:31 PM
 */
object ExtractInterestingPathsFromRexsterMultipleHosts {

    def main(args:Array[String]) {
        val hosts = args(0).split(",")
        val fromCui = args(1)
        val toCui = args(2)
        val maxLength = args(3).toInt
        val maxResults = args(4).toInt
        val relStore:CountStore = if(args.size > 5) {
            val relFile = new File(args(5))
            CountStore.fromFile(relFile)
        } else null

        extractInterestingPaths(hosts, fromCui, toCui, maxResults, maxLength, relStore)

        System.exit(0)
    }

    def extractInterestingPaths(hosts: Array[String], startCui: String, endCui: String, maxResults: Int, maxLength: Int, relStore:CountStore) {
        val graphClients = createGraphClients(hosts)
        //setup variable bindings
        graphClients.foreach(c => runScript[Object](c,"",Map("notAllowed" -> setAsJavaSet(notAllowedEdgeLabels))))

        //Cui -> graph ids
        val mapping = mutable.Map[Cui, Array[Long]]()

        def addMapping(cui: Cui) {
            mapping += cui -> graphClients.map(client => {
                val id = runScript[RList[java.lang.Long]](client, s"g.V('ui','$cui').id").getOrElse(new RList[java.lang.Long]())
                if(id.isEmpty)
                    lm1.toLong
                else
                    id.head.toLong
            }).toArray
        }

        addMapping(startCui)
        addMapping(endCui)

        def getForbiddenCuis(cui:Cui) = {
            graphClients.map(c => {
                val id = mapping(cui)(c.idx)
                if(id > -1)
                    runScript[RList[RList[String]]](c,s"g.v($id).bothE.filter{notAllowed.contains(it.label)}.transform{[it.label,it.inV.next().ui,it.outV.next().ui]}") match {
                        case Some(list) => list.foldLeft(Set[Cui]())(_ ++ _.tail.toSet).filterNot(_ == cui)
                        case None => Set[Cui]()
                    }
                else
                    Set[Cui]()
            }).reduce(_ ++ _).seq
        }

        //prioritize smaller scores
        val priorityQueue =
            new mutable.SynchronizedPriorityQueue[(Vector[MyElement], Set[Cui],Double,Cui)]()(new Ordering[(Vector[MyElement], Set[Cui], Double,Cui)] {
                def compare(x: (Vector[MyElement], Set[Cui], Double,Cui), y: (Vector[MyElement], Set[Cui], Double,Cui)) =
                    math.signum(y._3 - x._3).toInt
            })

        val startForbidden: Set[Cui] = getForbiddenCuis(startCui) ++ getForbiddenCuis(endCui)
        priorityQueue.enqueue((Vector[MyElement](CVertex(startCui)), startForbidden,0.0, endCui))
        priorityQueue.enqueue((Vector[MyElement](CVertex(endCui)), startForbidden,0.0, startCui))
        var result = List[(Vector[MyElement],Double)]()

        val localRelStore = new CountStore()

        val start = System.currentTimeMillis()
        var time: Long = 0
        val maxTime = 130 * 1000

        var fromPaths = Map[Cui,(Vector[MyElement], Set[Cui],Double)]()
        var toPaths = Map[Cui,(Vector[MyElement], Set[Cui],Double)]()

        while (!priorityQueue.isEmpty && result.size < maxResults && time < maxTime) {
            val (partialPath, forbiddenCuis, score, goalCui) = priorityQueue.dequeue()
            val lastCui = partialPath.head.asInstanceOf[CVertex].cui

            if (lastCui.equals(goalCui)) {
                result ::= ({ if(goalCui == startCui) partialPath else partialPath.reverse },score)
            }
            else if (partialPath.size / 2 < (maxLength+1) / 2) {

                def addEdge(fromCui:Cui, fromName:String, fromDegree:Int, fromSpecDegree:Int, fromDepth:Int, fromTotalDepth:Int,
                            toCui:Cui, toName:String, toDegree:Int, toSpecDegree:Int, toDepth:Int, toTotalDepth:Int,
                            label:String, out:Boolean) {
                    val (candCui,candName) = { if(out) (toCui,toName) else (fromCui,fromName) }

                    if(!forbiddenCuis.contains(candCui)) {
                        val globalEdgeCount = { if(relStore != null) relStore.getCount(label) else 1 }

                        //relation store serves also as a filter; everything that is not in there will not be considered
                        if(globalEdgeCount > 0) {
                            if (!mapping.contains(candCui)) {
                                addMapping(candCui)
                            }
                            if(!partialPath.exists {
                                case v: CVertex => v.cui == candCui
                                case _ => false
                            }) {
                                var weight =
                                    if(List(fromDegree,fromDepth,fromTotalDepth,toDegree,toDepth,toTotalDepth, fromSpecDegree, toSpecDegree).reduce(math.min) > 0)
                                        calcWeight(fromDepth, fromTotalDepth, fromDegree, fromSpecDegree, toDepth, toTotalDepth, toDegree, toSpecDegree)
                                    else
                                        weightEdge(label, mapping, fromCui, toCui, graphClients, localRelStore)


                                if(relStore != null) //idf weighting
                                    weight += relStore.getTotalCount / globalEdgeCount.toDouble

                                val newPath = (Vector(CVertex(candCui,candName), CEdge(fromCui, label, toCui)) ++ partialPath,
                                                    forbiddenCuis ++ getForbiddenCuis(candCui) , 1/(weight + 1/score*(partialPath.length/2))/(partialPath.length/2+1), goalCui)

                                if(goalCui == startCui)
                                    toPaths += lastCui -> (newPath._1,newPath._2,newPath._3)
                                else
                                    fromPaths += lastCui -> (newPath._1,newPath._2,newPath._3)

                                priorityQueue.enqueue(newPath)
                            }
                        }
                    }
                }

                //look at outEdges
                def explore(out:Boolean) {
                    val labelKey = BiggiUtils.LABEL
                    val dir = if(out) "outE" else "inE"
                    graphClients.flatMap(client => {
                        val fromId = mapping(lastCui)(client.idx)
                        if (fromId != -1) {
                            val exec: String = s"g.v($fromId).$dir.filter{!notAllowed.contains(it.$labelKey)}.transform{[it.$labelKey,it.f_degree,it.t_degree,it.outV.map.next(),it.inV.map.next()]}"
                                runScript[RList[RList[Object]]](client,exec).get
                            }
                        else
                            new RList[RList[Object]]()
                    }).groupBy(l => (l.get(0),
                                    l.get(3).asInstanceOf[java.util.Map[String, Object]].get(BiggiUtils.UI).asInstanceOf[Cui],
                                    l.get(4).asInstanceOf[java.util.Map[String, Object]].get(BiggiUtils.UI).asInstanceOf[Cui])).foreach {
                        case ((label,fromUI,toUI),list) => {
                            val fromSpecDegree = list.head(1)
                            val toSpecDegree = list.head(2)
                            val from = list.head(3).asInstanceOf[java.util.Map[String, Object]]
                            val to = list.head(4).asInstanceOf[java.util.Map[String, Object]]

                            addEdge(
                                fromUI,
                                from.getOrElse(BiggiUtils.TEXT, "").asInstanceOf[String],
                                from.getOrElse("degree", l0).asInstanceOf[java.lang.Long].toInt,
                                if (fromSpecDegree == null) 0 else fromSpecDegree.asInstanceOf[java.lang.Long].toInt,
                                from.getOrElse("depth", l0).asInstanceOf[java.lang.Long].toInt,
                                from.getOrElse("totalDepth", l0).asInstanceOf[java.lang.Long].toInt,
                                toUI,
                                to.getOrElse(BiggiUtils.TEXT, "").asInstanceOf[String],
                                to.getOrElse("degree", l0).asInstanceOf[java.lang.Long].toInt,
                                to.getOrElse("depth", l0).asInstanceOf[java.lang.Long].toInt,
                                to.getOrElse("totalDepth", l0).asInstanceOf[java.lang.Long].toInt,
                                if (toSpecDegree == null) 0 else toSpecDegree.asInstanceOf[java.lang.Long].toInt,
                                label.toString,
                                out)
                        }
                    }
                }

                //look at out edges
                explore(true)
                //look at in edges
                explore(false)
            }

            time = System.currentTimeMillis() - start
        }

        runScript[Object](graphClients,"g.commit()",Map[String,Object]())

        fromPaths.foreach{
            case (headCui, (path, unAllowed, score)) => {
                toPaths.withFilter(_._1 == headCui).foreach {
                    case (_,(toPath,toUnAllowed,toScore)) => {
                        if(unAllowed.intersect(toUnAllowed).isEmpty) {
                            //MATCH
                            result ::= (path.reverse ++ toPath.drop(1), (score / (path.length/2) + toScore / (toPath.length/2))*((path.length+toPath.length)/2))
                        }
                    }
                }
            }
        }

        result.foreach {
            case (path,score) => {
                printPath(path,score)
            }
        }

        println("Query time: " + time + " milliseconds")

        closeClients(graphClients)
    }


    private def calcWeight(fromDepth: Int, fromTotalDepth: Int, fromDegree: Int, fromSpecDegree: Int, toDepth: Int, toTotalDepth: Int, toDegree: Int, toSpecDegree: Int): Double = {
        0.25 * (fromDepth.toDouble / (fromTotalDepth * fromDegree) +
            toDepth.toDouble / (toTotalDepth * toDegree) +
            1 / fromSpecDegree +
            1 / toSpecDegree)
    }

    def weightEdge(eLabel:String, mapping:mutable.Map[Cui,Array[Long]], fromCui:Cui, toCui:Cui,clients:ParArray[GraphClient], localCounts:CountStore) = {
        val labelKey = BiggiUtils.LABEL
        def getStats(cui:Cui, out:Boolean): (Int, Int,Int,Int) = {
            val statistics = clients.map(client => {
                val dir = if (out) "outE" else "inE"
                val cdir = if (out) "inV" else "outV"

                val id = mapping(cui)(client.idx)
                if (id == -1)
                    (0, 0, 0, 0)
                else {
                    val exec: String =s"""g.v($id).transform{[ it.both.fill(new HashSet<String>()).size(), it.depth, it.totalDepth , it.$dir.filter{it.$labelKey == "$eLabel"}.$cdir.fill(new HashSet<String>()).size()]}.next()"""  //s"g.v($id).transform{[ it.$dir('$eLabel').count(), it.depth, it.totalDepth ]}.next()"
                    runScript[RList[java.lang.Long]](client,exec) match {
                        case Some(list) => {
                            var d, td = 0
                            if (list(1) != null) d = list(1).toInt
                            if (list(2) != null) td = list(2).toInt
                            val degree: Int = list(0).toInt
                            val specDegree = list(3).toInt

                            //localCounts.update(eLabel, specDegree)
                            (degree, d, td, specDegree)
                        }
                        case _ => (0, 0, 0, 0)
                    }
                }
            }).seq
            statistics.reduceLeft((acc, el) => (acc._1 + el._1, math.max(acc._2, el._2) , math.max(acc._3, el._3),acc._4 + el._4))
        }

        var (fDegree, fDepth, fTotalDepth, fSpecDegree) = getStats(fromCui,true)
        var (tDegree, tDepth, tTotalDepth, tSpecDegree) = getStats(toCui,false)

        if(fDegree == 0)
            fDegree = 1
        if(tDegree == 0)
            tDegree = 1

        //cache results in graph
        clients.foreach(client => {
            var id = mapping(fromCui)(client.idx)
            if(id > -1)
                runScript[Object](client,s"""g.v($id).sideEffect{it.degree = $fDegree; it.depth = $fDepth; it.totalDepth = $fTotalDepth}.outE.filter{it.$labelKey == "$eLabel"}.sideEffect{it.f_degree = $fSpecDegree}""")
            id = mapping(toCui)(client.idx)
            if(id > -1)
                runScript[Object](client,s"""g.v($id).sideEffect{it.degree = $tDegree; it.depth = $tDepth; it.totalDepth = $tTotalDepth}.filter{it.$labelKey == "$eLabel"}.sideEffect{it.t_degree = $tSpecDegree}""")

        })

        calcWeight(fDepth,fTotalDepth,fDegree,fSpecDegree,tDepth,tTotalDepth,tDegree,tSpecDegree)
    }
    
    def allowEdge(label:String) = !notAllowedEdgeLabels.contains(label)

}
