package biggi

import com.tinkerpop.blueprints.Edge
import java.lang
import scala.collection.mutable

/**
 * @author dirk
 *          Date: 11/20/13
 *          Time: 10:31 AM
 */
package object inference {

    // from, label, to
    protected[inference] type Cui = String
    protected[inference] trait MyElement
    protected[inference] case class CEdge(from:String, label:String, to:String) extends MyElement
    protected[inference] case class CVertex(cui:Cui,text:String = "") extends MyElement

    protected[inference] val lm1 = new lang.Long(-1)
    protected[inference] val l0 = new lang.Long(0)

    protected[inference] val synonymEdgeLabels =
        Set("same_as","clinically_similar","has_tradename","has_alias","gene_encodes_gene_product","mapped_from","SY","RL")
    protected[inference] val notAllowedEdgeLabels =
        synonymEdgeLabels ++ Set("isa","may_be_a","CHD","RN","RQ")

    protected[inference] def calcWeight(fromDepth: Int, fromTotalDepth: Int, fromDegree: Int, fromSpecDegree: Int, toDepth: Int, toTotalDepth: Int, toDegree: Int, toSpecDegree: Int): Double = {
        0.25 * (fromDepth.toDouble / (fromTotalDepth * fromDegree) +
            toDepth.toDouble / (toTotalDepth * toDegree) +
            1.0 / fromSpecDegree +
            1.0 / toSpecDegree)
    }

    protected[inference] def allowEdge(e:Edge) = !notAllowedEdgeLabels.contains(e.getLabel)



    protected[inference] def printPath(path: Vector[MyElement], score: Double) {
        var prevVertex = ""
        println(path.map {
            case vertex: CVertex => prevVertex = vertex.cui; if (vertex.text != null && vertex.text != "") vertex.text else vertex.cui
            case edge: CEdge => edge.label + {
                if (edge.to == prevVertex) "^-1" else ""
            }
        }.reduce(_ + " , " + _) + " " + score)
    }

    protected[inference] type Path = Node

    protected[inference] class Node(val cui:Cui,val parent:Option[Node],var children:Set[Node] = Set[Node](),private val forbiddenCuis:Set[Cui] = Set[Cui]()) extends Cloneable{
        lazy val size:Int = parent match {
            case None => 1
            case Some(p) => 1+ p.size
        }

        def getForbiddenCuis:Set[Cui] = forbiddenCuis ++ parent.fold(Set[Cui]())(p => p.getForbiddenCuis)

        def allowedOnPath(cui:Cui):Boolean = !forbiddenCuis.contains(cui) && { parent match {
            case None => true
            case Some(p) => p.allowedOnPath(cui)
        } }

        def getCuiPath:List[Cui] = cui :: parent.fold(List[Cui]())(_.getCuiPath)

        def getBottomUpPath:List[Node] = this :: parent.fold(List[Node]())(_.getBottomUpPath)

        def clone(parent:Option[Node]) = new Node(cui,parent,Set[Node](),forbiddenCuis)
    }

    protected[inference] class SearchTree(val root:Node) extends Traversable[List[Node]] {
        assert(root.parent == None)
        var cuiToNode = Map[Cui,Set[Node]](root.cui -> Set(root))

        def insert(node:Node):Node = {
            cuiToNode += node.cui -> (cuiToNode.getOrElse(node.cui,Set[Node]()) + node)
            node.parent.foreach(_.children += node)
            node
        }

        def insert(cui:Cui, parent:Path, forbidden:Set[Cui]):Node = {
            val newForbidden = forbidden -- parent.getForbiddenCuis
            val node = new Node(cui, Some(parent), Set[Node](),newForbidden)
            insert(node)
        }

        def insert(path:List[Node]) {
            if(path.head.cui == root.cui) {
                var current = root
                path.tail.dropWhile(node => {
                    val child = current.children.find(_.cui == node)
                    if(child.isDefined) {
                        current = child.get
                        true
                    } else false
                }).foreach(node => {
                    val newNode = insert(node.clone(Some(current)))
                    current = newNode
                })
            }
        }

        def foreach[U](f: (List[Node]) => U) = {
            val stack = new mutable.Stack[Node]()
            stack.push(root)

            while(!stack.isEmpty) {
                val current = stack.pop()
                if(current.children.isEmpty) {
                    f(current.getBottomUpPath.reverse)
                }
                else
                    stack.pushAll(current.children)
            }
        }

        def getLeafs = cuiToNode.values.flatMap(_.filter(_.children.isEmpty))
    }
}
