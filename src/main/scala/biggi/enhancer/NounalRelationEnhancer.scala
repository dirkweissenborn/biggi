package biggi.enhancer

import biggi.model.AnnotatedText
import biggi.model.annotation.{SemanticRoleLabel, Sentence}
import biggi.dependency.{DepEdge, DepNode, DependencyTree}
import scalax.collection.edge.Implicits._
import scalax.collection.Graph
import biggi.model.annotation.{Sentence, SemanticRoleLabel}
import biggi.dependency.{DependencyTree, DepNode, DepEdge}
import biggi.model.AnnotatedText

/**
 * @author dirk
 * Date: 4/25/13
 * Time: 10:54 AM
 */
object NounalRelationEnhancer extends TextEnhancer{

    val templateTree = Graph(((".*",0)~+>(".*",1))("(nsubj|.obj)"),((".*",1)~+>(".*",2))("prep"),((".*",2)~+>(".*",3))("pobj")  )

    protected def pEnhance(text: AnnotatedText) {
        text.getAnnotations[Sentence].foreach(sentence => {
            val tree: DependencyTree = sentence.dependencyTree
            val intersections = tree.matchTreeToTemplate(templateTree)
            intersections.foreach(intersection => {
                try {
                    val prepEdge: DepEdge[tree.graph.NodeT] = intersection.find(edge => edge.label.equals("prep")).get.edge
                    val nsubj = prepEdge.from.value.asInstanceOf[DepNode]
                    val prep = prepEdge.to.value.asInstanceOf[DepNode]

                    if (prep.getLabel.equals("of")) {
                        val pobj = intersection.find( edge => edge.label.equals("pobj")).get.edge.to.value.asInstanceOf[DepNode]
                        pobj.nodeHead.srls ++= List(new SemanticRoleLabel(nsubj.nodeHead.position,"A1"))
                    }
                    else {
                        prep.nodeHead.srls ++= List(new SemanticRoleLabel(nsubj.nodeHead.position,"AM"))
                    }
                }
                catch {
                    case e => e.printStackTrace()
                }
            })
        })
    }
}
