package biggi.dependency

import biggi.model.annotation.Token
/**
 * @author dirk
 * Date: 4/17/13
 * Time: 12:13 PM
 */
class DepNode(var tokens:List[Token], var nodeHead:Token, var optional:Boolean = false) {
    tokens = tokens.sortBy(_.position)

    def this(token:Token) = this(List(token),token)

    override def hashCode() = tokens.hashCode() + nodeHead.hashCode()

    override def toString = "DepNode["+tokens.map(_.coveredText).mkString(" ")+"]"

    def getLabel = tokens.map(_.lemma).mkString(" ")

    override def equals(obj:Any) = obj match {
        case depNode:DepNode => {
            this.tokens.equals(depNode.tokens)  && depNode.nodeHead.equals(this.nodeHead)
        }
        case _ => false
    }
}

object DepNode {

    def equalDependencyTag(node1:DepNode, node2:DepNode):Boolean = node1.nodeHead.depTag.tag.equals(node2.nodeHead.depTag.tag)

}
