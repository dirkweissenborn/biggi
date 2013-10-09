package biggi.model.annotation

import biggi.model.{AnnotatedText, Span}
import biggi.dependency.{DepNode, DependencyTree}

/**
 * @author dirk
 *          Date: 4/15/13
 *          Time: 1:32 PM
 */
class Sentence(spans:Array[Span], context:AnnotatedText) extends Annotation(spans,context) {
    if (spans.size > 1) {
        throw new IllegalArgumentException("Section can just span over one span range!")
    }

    def this(begin:Int,end:Int,context:AnnotatedText) = this(Array(new Span(begin,end)),context)

    def dependencyTree = {
        if (_dependencyTree==null)
            _dependencyTree = DependencyTree.fromSentence(this)
        _dependencyTree
    }

    private var _dependencyTree: DependencyTree = null

    def printRoleLabels = {
        this.getTokens.flatMap(token => {
            token.srls.map(srl => {
                var res = ""
                try {
                    val head: DepNode = dependencyTree.find((node: DepNode) => node.nodeHead.position.equals(srl.head)).get
                    val depNode: DepNode = dependencyTree.find((node: DepNode) => node.nodeHead.equals(token)).get

                    res = srl.label + "("
                    res += head.tokens.map(_.lemma).mkString(" ") + ","
                    if (depNode.nodeHead.depDepth > head.nodeHead.depDepth) {
                        val subtree = dependencyTree.getSubtree(depNode)
                        res += subtree.nodes.toList.flatMap((node: subtree.NodeT) => node.value.asInstanceOf[DepNode].tokens).sortBy(_.position).map(_.coveredText).mkString(" ")
                    }
                    else {
                        res += depNode.tokens.map(_.lemma).mkString(" ")
                    }
                    res += ")"
                }
                catch {
                    case e => e.printStackTrace()
                }
                res
            })
        }).mkString("\t")
    }
}


