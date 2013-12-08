package biggi.inference

import java.io.{PrintStream, FileOutputStream, File}
import biggi.util.BiggiUtils
import com.thinkaurelius.titan.core.{TitanGraph, TitanFactory}
import scala.collection.JavaConversions._
import scala.collection.{mutable, GenSeq}
import com.tinkerpop.blueprints._
import com.tinkerpop.gremlin.scala._
import java.util
import scala.util.Random
import biggi.model.CountStore

/**
 * @author dirk
 *          Date: 10/9/13
 *          Time: 3:31 PM
 */
object ExtractInterestingPathsFromGraphs {
    var edgeStats = Map[CEdge,Map[String,Int]]()
    var graphs:GenSeq[(TitanGraph,util.HashMap[Cui,java.lang.Long])] = null
    var umlsGraph:(TitanGraph,util.HashMap[Cui,java.lang.Long]) = null
    var evalPolicy:EvalPolicy = TestEval

    def main(args:Array[String]) {
        val graphsDir = new File(args(0))
        val umlsDir = new File(args(1))
        val startCui = args(2)
        val endCui = args(3)
        val maxLength = args(4).toInt
        val maxNrEdgePaths = args(5).toDouble
        val maxTime = args(6).toInt*1000
        val degreeStore = CountStore.fromFile(new File(args(7)))

        if(args.size > 8)
            evalPolicy = args(8) match {
                case "scoring" => if(args.size > 9) ScoringEval(new File(args(9))) else ScoringEval()
                case "no-scoring" => if(args.size > 9) NoScoringEval(new File(args(9))) else NoScoringEval()
                case _ => TestEval
            }

        umlsGraph = {
            val conf = BiggiUtils.getGraphConfiguration(umlsDir)
            conf.setProperty("storage.transactions","false")
            conf.setProperty("storage.batch-loading","true")
            val g = TitanFactory.open(conf)
            if(!g.getIndexedKeys(classOf[Vertex]).contains("degree"))
                g.makeKey("degree").dataType(classOf[java.lang.Integer]).indexed(classOf[Vertex]).make()
            if(!g.getIndexedKeys(classOf[Edge]).contains("f_degree")) {
                g.makeKey("f_degree").dataType(classOf[java.lang.Integer]).indexed(classOf[Edge]).make()
                g.makeKey("t_degree").dataType(classOf[java.lang.Integer]).indexed(classOf[Edge]).make()
            }
            if(!g.getIndexedKeys(classOf[Vertex]).contains("depth")) {
                g.makeKey("depth").dataType(classOf[java.lang.Integer]).indexed(classOf[Vertex]).make()
                g.makeKey("totalDepth").dataType(classOf[java.lang.Integer]).indexed(classOf[Vertex]).make()
            }
            g.commit()

            (g, BiggiUtils.getCuiToID(umlsDir))
        }

        graphs = graphsDir.listFiles().toSeq.map(graphDir => {
            val conf = BiggiUtils.getGraphConfiguration(graphDir)
            conf.setProperty("storage.transactions","false")
            conf.setProperty("storage.batch-loading","true")
            val g = TitanFactory.open(conf)
            if(!g.getIndexedKeys(classOf[Vertex]).contains("degree"))
                g.makeKey("degree").dataType(classOf[java.lang.Integer]).indexed(classOf[Vertex]).make()
            if(!g.getIndexedKeys(classOf[Edge]).contains("f_degree")) {
                g.makeKey("f_degree").dataType(classOf[java.lang.Integer]).indexed(classOf[Edge]).make()
                g.makeKey("t_degree").dataType(classOf[java.lang.Integer]).indexed(classOf[Edge]).make()
            }
            g.commit()

            (g, BiggiUtils.getCuiToID(graphDir))
        }).+:(umlsGraph).par

        if(!graphs.exists(g => g != umlsGraph && g._2.containsKey(startCui)) || !graphs.exists(g => g != umlsGraph && g._2.containsKey(endCui))) {
            println("Either start or end concept not found!")
            System.exit(1)
        }

        def getForbiddenCuis(cui:Cui) = {
            val (graph,cuiId) = umlsGraph
            (cuiId.get(cui) match {
                case null => Set[Cui]()
                case id =>
                    var set = Set[Cui]()
                    graph.v(id).->.bothE().filter((e:Edge) => notAllowedEdgeLabels.contains(e.getProperty[String](BiggiUtils.LABEL))).
                      map((e:Edge) => {
                        val v = if(e.getVertex(Direction.IN).getId == id) e.getVertex(Direction.OUT)
                                else e.getVertex(Direction.IN)
                        set += v.getProperty[String](BiggiUtils.UI)
                        v
                    }).bothE().filter((e:Edge) => synonymEdgeLabels.contains(e.getProperty[String](BiggiUtils.LABEL))).
                      sideEffect((e:Edge) => {
                        set += e.getVertex(Direction.IN).getProperty[String](BiggiUtils.UI)
                        set += e.getVertex(Direction.OUT).getProperty[String](BiggiUtils.UI)
                    }).toScalaList()
                    set
            }) ++ Set(cui)
        }

        val priorityQueue = new mutable.PriorityQueue[(Path,Cui)]()(new Ordering[(Path,Cui)] {
            def compare(x: (Path, Cui), y: (Path, Cui)) = math.signum(y._1.size - x._1.size)
        })

        val startForbidden: Set[Cui] = getForbiddenCuis(startCui) ++ getForbiddenCuis(endCui)
        val startPath = new Path(startCui,None,Set[Node](),startForbidden - endCui)
        priorityQueue.enqueue((startPath, endCui))
        val startSearchTree = new SearchTree(startPath)

        val start = System.currentTimeMillis()

        def allowEdge(forbiddenCuis:Set[Cui], cui:Cui, e:CEdge): Boolean = {
            val candCui =  if (e.from == cui) e.to else e.from
            !forbiddenCuis.contains(candCui) &&
                degreeStore.getCount(candCui) <= 100000 &&
                graphs.exists(g => g != umlsGraph && g._2.containsKey(candCui)) &&
                !notAllowedEdgeLabels.contains(e.label)
        }

        val resultSearchTree = new SearchTree(new Path(startCui,None,Set[Node]()))

        def search(f:Path => Unit,maxTime:Long,searchTree:SearchTree = null) {
            val start = System.currentTimeMillis()
            var time = l0
            while (!priorityQueue.isEmpty && time < maxTime) {
                val (partialPath, goalCui) = priorityQueue.dequeue()

                //find neighbours
                val neighbours =
                    explore(partialPath.cui,
                            e => { val fc = partialPath.getForbiddenCuis; allowEdge(fc, partialPath.cui, e) },
                            goalCui)

                neighbours.foreach(candCui => {
                    val newForbidden = {
                        if(partialPath.size < (maxLength + 1) / 2)
                            getForbiddenCuis(candCui)
                        else
                            Set[Cui]()
                    }
                    val newPath = if(searchTree != null) searchTree.insert(candCui,partialPath,newForbidden)
                                  else new Path(candCui,Some(partialPath),Set[Node](),newForbidden)

                    if (candCui.equals(goalCui)) {
                        resultSearchTree.insert(if (goalCui == startCui) newPath.getBottomUpPath else newPath.getBottomUpPath.reverse)
                    } else if (!newForbidden.contains(goalCui)) {
                        f(newPath)

                        if(newPath.size-1 < (maxLength + 1) / 2)
                            priorityQueue.enqueue((newPath,goalCui))
                    }
                })
                time = System.currentTimeMillis() - start
            }
        }

        search(_ => {}, maxTime/2, startSearchTree)

        val endPath = new Path(endCui,None,Set[Node](),startForbidden - startCui)
        priorityQueue.enqueue((endPath, startCui))

        priorityQueue.clear()
        priorityQueue.enqueue((endPath, startCui))

        search({ case toPath =>
            startSearchTree.cuiToNode.get(toPath.cui).foreach(_.foreach {
                case path => {
                    val nodePath = path.getBottomUpPath
                    val toNodePath = toPath.getBottomUpPath
                    if(!nodePath.tail.dropRight(1).exists(node => !toPath.allowedOnPath(node.cui)) &&
                        !toNodePath.tail.dropRight(1).exists(node => !path.allowedOnPath(node.cui))) {
                        //MATCH
                        resultSearchTree.insert(nodePath.reverse ++ toNodePath.tail)
                    }
                }
            })
        }, maxTime/2)
        val searchEnd = System.currentTimeMillis()

        val nrOfPaths = resultSearchTree.getLeafs.size

        val edgePaths = selectEdgePaths(resultSearchTree, maxNrEdgePaths, nrOfPaths, degreeStore)
        val selectionEnd = System.currentTimeMillis()

        evalPolicy.process(edgePaths)

        println("Total time: " + (System.currentTimeMillis() - start) + " milliseconds")
        println("Search time: " + (searchEnd - start) + " milliseconds")
        println("Selection time: " + (selectionEnd - searchEnd) + " milliseconds")
        println(s"Nr of paths found: $nrOfPaths")

        graphs.foreach(_._1.shutdown())
        System.exit(0)
    }

    /** gather edges; each "vertex-path" (considering only vertices) can have multiple "edge-paths".
     *  Each "vertex-path" receives the should be represented with equal nr of edge-paths, s.t. no "vertex-path",
     *  dominates the others. Each vertex-path is allowed to create total/nrPaths edge-paths doing a random walk. 
     */
    def selectEdgePaths(resultSearchTree: SearchTree, maxNrEdgePaths: Double, nrVertexPaths: Int, degreeStore:CountStore): List[List[(Edge, Boolean)]] = {
        resultSearchTree.foldLeft(List[List[(Edge, Boolean)]]()) {
            case (acc, vertexPath) =>
                var from = vertexPath.head
                var tail = vertexPath.tail
                var selectedPaths = List[List[(Edge, Boolean)]]()

                //randomly select edges from edge pool, and at least one per concept pair (otherwise no path)
                val length = vertexPath.size - 1
                //is reversed, extract paths from all possibilities
                //nr of edges that can be extracted for each connection without the obligatory one
                val nrOfPossEdges = math.pow(maxNrEdgePaths / nrVertexPaths - 1, 1.0 / length)

                while (!tail.isEmpty) {
                    val to = tail.head
                    val edges = graphs.flatMap {
                        case (graph, cuiId) =>
                            (cuiId.getOrElse(from.cui, lm1), cuiId.getOrElse(to.cui, lm1)) match {
                                case (fromId, toId) if fromId >= 0 && toId >= 0 =>
                                    if(degreeStore.getCount(from.cui) <= degreeStore.getCount(to.cui))
                                        graph.getVertex(fromId).bothE.filter((e: Edge) => e.getVertex(Direction.IN).getId == toId || e.getVertex(Direction.OUT).getId == toId)
                                            .toScalaList()
                                    else
                                        graph.getVertex(toId).bothE.filter((e: Edge) => e.getVertex(Direction.IN).getId == fromId || e.getVertex(Direction.OUT).getId == fromId)
                                            .toScalaList()
                                case (_, _) => List[Edge]()
                            }
                    }.foldLeft(List[(Edge,Boolean)]())((acc,edge) => {
                        if(acc.exists(_._1.getProperty[String](BiggiUtils.LABEL) == edge.getProperty[String](BiggiUtils.LABEL)))
                            acc
                        else
                            (edge, edge.getVertex(Direction.OUT).getProperty[String](BiggiUtils.UI) == from.cui) :: acc
                    }).toVector

                    from = to
                    tail = tail.tail

                    //select edges
                    val nrOfEdges = edges.length -1

                    //(maxNrOfEdgePaths/nrOfVertexPaths - 1)^(1/length)/nrOfEdges(between from and to)
                    val selectedEdges = if (nrOfEdges <= 1 || nrOfPossEdges >= nrOfEdges)
                        edges
                    else {
                        val possEdge = nrOfPossEdges/nrOfEdges
                        val selected = edges.get(Random.nextInt(nrOfEdges+1))
                        //first select at least one
                        edges.filter(_!=selected && Random.nextDouble() <= possEdge).:+(selected)
                    }

                    if (selectedPaths.isEmpty)
                        selectedPaths = selectedEdges.map(e => List(e)).toList
                    else
                        selectedPaths = selectedPaths.flatMap(path => selectedEdges.map(e => {
                            if(!path.exists(_._1.getProperty[String](BiggiUtils.SOURCE) == e._1.getProperty[String](BiggiUtils.SOURCE)))
                                Some(e :: path)
                            else None
                        }).flatten )
                }

                acc ++ selectedPaths.mapConserve(_.reverse)
            }
    }

    def explore(cui:Cui, allowEdge: CEdge => Boolean, goalCui:Cui) = {
        val constant = 100000.0
        val neighbourCount = getNeighbours(allowEdge,cui)
        val size = neighbourCount.size
        val prob = math.sqrt(constant+size)/size

        if(prob >= 1) {
            neighbourCount.keySet
        } else {
            var res = Set[Cui]()
            neighbourCount.foreach{ case (otherCui,_) => if(Random.nextDouble() <= prob) res += otherCui }
            if(neighbourCount.contains(goalCui))
                res.+(goalCui)
            else
                res
        }
    }

    def weightEdge(edge:Edge) = {
        val from = edge.getVertex(Direction.OUT)
        val to = edge.getVertex(Direction.IN)

        val fDepth = from.getProperty[Integer]("depth")
        val tDepth = to.getProperty[Integer]("depth")
        val fTotalDepth = from.getProperty[Integer]("totalDepth")
        val tTotalDepth = to.getProperty[Integer]("totalDepth")

        val fDegree = from.getProperty[Integer]("degree")
        val tDegree = to.getProperty[Integer]("degree")

        val fSpecDegree = edge.getProperty[Integer]("f_degree")
        val tSpecDegree = edge.getProperty[Integer]("t_degree")

        calcWeight(fDepth,fTotalDepth,fDegree,fSpecDegree,tDepth,tTotalDepth,tDegree,tSpecDegree)
    }
    
    def getNeighbours(allowEdge: CEdge => Boolean,cui:Cui) = {
        val edges = graphs.map {
            case (g, cuiIdMap) =>
                cuiIdMap.get(cui) match {
                    case null => Set[CEdge]()
                    case id =>
                        var edges = Set[CEdge]()
                        g.getVertex(id).getEdges(Direction.BOTH).foreach(e => {
                            val edge = CEdge(e.getVertex(Direction.OUT).getProperty[Cui](BiggiUtils.UI),
                                e.getProperty[String](BiggiUtils.LABEL),
                                e.getVertex(Direction.IN).getProperty[Cui](BiggiUtils.UI))
                            if(allowEdge(edge))
                                edges += edge
                        })
                        edges
                }
        }.reduce(_ ++ _)

        val neighbourCount = mutable.Map[String,Double]()
        edges.foreach(e => {
            val otherCui = if(e.from == cui) e.to
                           else e.from
            neighbourCount.getOrElseUpdate(otherCui,0.0)
            neighbourCount(otherCui) += 1.0
        })

        neighbourCount
    }

    protected[ExtractInterestingPathsFromGraphs] trait EvalPolicy {
        def process(paths: List[List[(Edge,Boolean)]])
        
        protected def printPath(path: List[(Edge, Boolean)],withVertices:Boolean = false): String = {
            val printPath = path.map {
                case (e, out) =>
                    e.getProperty[String](BiggiUtils.LABEL) + {
                        if (out)
                            " " +
                                {if(withVertices)
                                    e.getVertex(Direction.IN).getProperty[String](BiggiUtils.UI) + "(" + e.getVertex(Direction.IN).getProperty[String](BiggiUtils.TEXT) + ")"
                                else ""}
                        else
                            "^-1 " +
                                {if(withVertices)
                                    e.getVertex(Direction.OUT).getProperty[String](BiggiUtils.UI) + "(" + e.getVertex(Direction.OUT).getProperty[String](BiggiUtils.TEXT) + ")"
                                else ""}
                    }
            }.mkString(" | ")
            printPath
        }

        protected def addParameters(paths: List[List[(Edge, Boolean)]]) {
            //calculate and cache stats at edges and vertices
            paths.flatMap(_.flatMap(e => List((e._1.getVertex(Direction.OUT).getProperty[String](BiggiUtils.UI), e._1),
                (e._1.getVertex(Direction.IN).getProperty[String](BiggiUtils.UI), e._1)))).groupBy(_._1).foreach {
                case (cui, edges) =>
                    val labels = new util.HashMap[String, Int]
                    edges.foreach(e => labels.update(e._2.getProperty[String](BiggiUtils.LABEL), 0))
                    var total = 0
                    var d, td = new Integer(1)
                    graphs.seq.foreach {
                        case (graph, cuiId) =>
                            cuiId.getOrElse(cui, lm1) match {
                                case id if id >= l0 =>
                                    val vertex = graph.getVertex(id)
                                    if (vertex.getPropertyKeys.contains("depth"))
                                        d = vertex.getProperty[Integer]("depth")
                                    if (vertex.getPropertyKeys.contains("totalDepth"))
                                        td = vertex.getProperty[Integer]("totalDepth")
                                    vertex.getEdges(Direction.BOTH).foreach(e => {
                                        val l = e.getProperty[String](BiggiUtils.LABEL)
                                        if (labels.containsKey(l))
                                            labels(l) += 1
                                        total += 1
                                    })
                                case _ => //nothing
                            }
                    }

                    edges.foreach(e =>
                        if (e._2.getVertex(Direction.OUT).getProperty[String](BiggiUtils.UI) == cui)
                            e._2.setProperty("f_degree", labels(e._2.getProperty[String](BiggiUtils.LABEL)))
                        else
                            e._2.setProperty("t_degree", labels(e._2.getProperty[String](BiggiUtils.LABEL)))
                    )
                    graphs.foreach {
                        case (graph, cuiId) =>
                            cuiId.getOrElse(cui, lm1) match {
                                case id if id >= l0 =>
                                    val vertex = graph.getVertex(id)
                                    vertex.setProperty("degree", total)
                                    vertex.setProperty("depth", d)
                                    vertex.setProperty("totalDepth", td)
                                case _ => //nothing
                            }
                    }
            }
            graphs.foreach(_._1.commit())
        }
    }

    protected[ExtractInterestingPathsFromGraphs] object TestEval extends EvalPolicy {
        def process(paths: List[List[(Edge,Boolean)]]) {
            //addParameters(paths)

            paths.foreach(path => {
                println(printPath(path,true))
            })
        }
    }

    protected[ExtractInterestingPathsFromGraphs] case class ScoringEval(outFile:File = null) extends EvalPolicy {
        def process(paths: List[List[(Edge,Boolean)]]) {
            addParameters(paths)

            val scoredPaths = paths.map(path => {
                val weight = path.map(e => weightEdge(e._1)).sum/path.size
                val printablePath = printPath(path)
                (printablePath,weight)
            })
            val outStream = if(outFile == null) System.out else new PrintStream(new FileOutputStream(outFile))
            scoredPaths.sortBy(-_._2).foreach(path => outStream.println("%1.4f\t%s".format(path._2,path._1)))
            if(outFile != null)
                outStream.close()
        }
    }

    protected[ExtractInterestingPathsFromGraphs] case class NoScoringEval(outFile:File = null) extends EvalPolicy {
        def process(paths: List[List[(Edge, Boolean)]]) {
            val outStream = if(outFile == null) System.out else new PrintStream(new FileOutputStream(outFile))
            paths.foreach(path => {
                outStream.println(printPath(path))
            })
            if(outFile != null)
                outStream.close()
        }
    }
}
