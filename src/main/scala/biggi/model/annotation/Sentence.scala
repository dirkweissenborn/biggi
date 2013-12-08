package biggi.model.annotation

import biggi.model.{AnnotatedText, Span}

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

    private class DependencyTree {
        val root = getTokens.find(_.depTag.dependsOn == 0).get

        def getSubTree(token:Token):List[Token] = {
            val children = getTokens.filter(_.depTag.dependsOn == token.position).sortBy(_.position)
            val (head,tail) = children.span(_.position < token.position)
            head.flatMap(c => getSubTree(c)) ++ List(token) ++ tail.flatMap(c => getSubTree(c))
        }
    }

    lazy val dependencyTree = new DependencyTree

    def printRoleLabels = {
        this.getTokens.flatMap(token => {
            token.srls.map(srl => {
                var res = ""
                try {
                    val headToken = getTokens.find(token => token.position.equals(srl.head)).get

                    res = srl.label + "("
                    res += headToken.lemma
                    if (token.depDepth > headToken.depDepth) {
                        res += token.lemma
                    }
                    else {
                        res += dependencyTree.getSubTree(token).map(_.lemma).mkString(" ")
                    }
                    res += ")"
                }
                catch {
                    case e:Throwable => e.printStackTrace()
                }
                res
            })
        }).mkString("\t")
    }

    def prettyPrint = {
        getTokens.map(token => {
            ("\t" * token.depDepth)+ token.depTag.dependsOn +token.lemma + ":" + token.depTag.tag + ":" + token.position
        }).mkString("\n")
    }
}


