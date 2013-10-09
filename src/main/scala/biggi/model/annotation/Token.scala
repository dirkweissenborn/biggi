package biggi.model.annotation

import biggi.model.{Span, AnnotatedText}
import biggi.dependency.{DepNode}

/**
 * @author dirk
 *         Date: 4/8/13
 *         Time: 2:09 PM
 */
class Token(spans:Array[Span],context:AnnotatedText, val position:Int) extends Annotation(spans,context) {
    if (coveredText.contains(" "))
        throw new IllegalArgumentException("Token cannot have whitespaces!")

    def this(begin:Int,end:Int, context:AnnotatedText, position:Int) = this(Array(new Span(begin,end)),context,position)

    var depTag:DepTag = DepTag("",0)

    var lemma:String = ""

    var posTag:String = ""

    var tokenType:String = ""

    var srls = List[SemanticRoleLabel]()

    private var _sentence:Sentence = null
    def sentence = {
        if(_sentence == null)
            _sentence = getContext.getAnnotations[Sentence].find(_.contains(begin,end)).get
        _sentence
    }

    private var _depDepth:Int = -1

    def depDepth:Int = {
        if(_depDepth <= 0)
            if (depTag.dependsOn == 0)
                _depDepth = 1
            else
                _depDepth = sentence.getTokens.find(_.position.equals(depTag.dependsOn)) match {
                    case Some(token) => 2 + token.depDepth
                    case None => 1
                }

        _depDepth -1
    }

    private type Arg = (String,Token)
    private type Pred = (String,Token)
    private type SRole = String

    def prettyPrintRelations: List[(SRole, Pred, Arg)] = {
        srls.map(srl => {
            val tree = sentence.dependencyTree

            val pred = tree.find( (node: DepNode) => node.nodeHead.position.equals(srl.head)).get

            val subRoot: DepNode = tree.find((node: DepNode) => node.tokens.contains(this)).get
            val subtree = tree.getSubtree(subRoot)
            val arg = subtree.nodes.toList.flatMap((node: subtree.NodeT) => node.value.asInstanceOf[DepNode].tokens).sortBy(_.position).map(_.coveredText).mkString(" ")

            (srl.label,
                (pred.tokens.map(_.lemma).mkString(" "), pred.nodeHead),
                (arg,subRoot.nodeHead))
        })
    }
}

case class DepTag(var tag:String, var dependsOn:Int)

case class SemanticRoleLabel(head:Int, label:String)

object Token {

    val NUM_TYPE =  "NUM"
    val PUNCTUATION_TYPE = "PUNCTUATION"
    val SYMBOL_TYPE = "SYMBOL"
    val WORD_TYPE = "WORD"

}
