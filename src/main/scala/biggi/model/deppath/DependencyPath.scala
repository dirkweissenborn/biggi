package biggi.model.deppath

import scala.util.parsing.combinator.{PackratParsers, RegexParsers}

/**
 * @author dirk
 *          Date: 12/9/13
 *          Time: 2:02 PM
 */
class DependencyPath(val depNodes:List[DependencyNode], val inverse:Boolean = false) {
    override def equals(that:Any):Boolean = that match {
        case that:DependencyPath => this.depNodes.equals(that.depNodes)
        case _ => false
    }

    override def toString = {
        { if(getFeature[Boolean](DependencyPath.NEG_FEATURE)(false))"neg_" else "" } + depNodes.map(_.toString).mkString + { if(inverse) "^-1" else "" }
    }

    def toShortString = {
        var string = toString
        var newS = string.replaceAll("""\([^()]*\)""","")
        while(newS != string) {
            string = newS
            newS = newS.replaceAll("""\([^()]*\)""","")
        }
        string
    }

    private var features = Map[String,Any]()

    def addFeature(name:String, value:Any) {
        features += name -> value
    }

    def getFeature[T](name:String)(implicit orElse:T = null):T = features.getOrElse(name, orElse).asInstanceOf[T]
}

object DependencyPath extends RegexParsers {
    override def skipWhitespace = false

    final val NEG_FEATURE = "neg"

    def fromString(depPathStr:String, inverse:Boolean = false):DependencyPath = {
        val cleaned: String = depPathStr.replaceAll( """(?<= ):""", "")
        this.parseAll(simpleRelation,cleaned) match {
            case Success(depPath,_) => depPath
            case other => parseAll(depPath(inverse), cleaned) match {
                case Success(depPath,_) => depPath
                case other => println(other); null
            }
        }
    }

    private val symbol: Parser[String] = """(\d+(\.\d+)?:\d+(\.\d+)?)|[^ :()]+""".r
    private val depTag: Parser[String] = """[a-zA-Z]+""".r

    private def depPath(inverse:Boolean):Parser[DependencyPath] = (depNode(Some(true)) <~ " -> ").* ~ rootNode ~ (" <- ".r ~> depNode(Some(false))).* ~ "^-1".? ^^ {
        case upNodes ~ root ~ downNodes ~ invSign =>
            val nodes = upNodes ++ List(root) ++ downNodes
            def negation(node:DependencyNode):Boolean = node.tag == "neg" || node.lemma == "no"
            val neg = nodes.count(n => negation(n) || n.attr.exists(negation)) % 2 == 1
            val dp = new DependencyPath(nodes,inverse || invSign.isDefined)
            dp.addFeature(NEG_FEATURE,neg)
            dp
    }

    private def depNode(up:Option[Boolean]):Parser[DependencyNode] = (symbol.? <~ ":+".r).? ~ depTag.? ~ attr.? ^^ {
        case lemmaOption ~ tagOption ~ attr =>
            val lemma = lemmaOption.getOrElse(None).getOrElse("")
            DependencyNode(lemma,tagOption.getOrElse(""),attr.getOrElse(List[DependencyNode]()),up)
    }

    private def rootNode:Parser[DependencyNode] = symbol.? ~ attr.? ^^ {
        case lemma ~ attr =>
            DependencyNode(lemma.getOrElse(""),"",attr.getOrElse(List[DependencyNode]()),None)
    }

    private def attr: Parser[List[DependencyNode]] = "(" ~> rep1sep(depNode(None), " ") <~ ")"

    private def simpleRelation: Parser[DependencyPath] = symbol ~ ( ":+".r ~> depTag).? ^^ {
        case rel ~ tag => new DependencyPath(List(DependencyNode(rel,tag.getOrElse("rel"))))
    }


    //faster than parsing
    def removeAttributes(depPath:String, withNeg:Boolean = false, inverse:Boolean = false) = {
        var openParenths = 0
        var directAttributes = ""
        var result = depPath.filter((c:Char) => {
            c match {
                case '(' => openParenths += 1
                case ')' => openParenths -= 1
                case _ =>
            }
            if(openParenths == 1 && c != '(')
                directAttributes += c
            openParenths == 0 && c != ')'
        })

        if(withNeg && !""":neg|[^a-zA-Z]no?[^a-zA-Z]""".r.findAllIn(directAttributes).matchData.isEmpty)
            result = "neg_"+result

        if(inverse)
            result += "^-1"

        result
    }

    def main(args:Array[String]) {
        var p = parseAll(simpleRelation,"relation:rel")
        println(p)
        p = parseAll(depPath(false),"(non-hydrolyzable:amod paf:nn(:npadvmod(1:num))) -> add <- to:prep <- pobj")
        println(p)
        p = parseAll(depPath(false),"(:hmod) -> excrete <- dobj")
        println(p)
    }
}

case class DependencyNode(lemma:String,tag:String,attr:List[DependencyNode] = List[DependencyNode](), goingUp:Option[Boolean] = None) {
    override def equals(that:Any):Boolean = that match {
        case that:DependencyNode => this.lemma == that.lemma && this.tag == that.tag && goingUp == that.goingUp
        case _ => false
    }

    override def toString = {
        val start = if(goingUp == Some(false)) " <- " else ""
        val end = if(goingUp == Some(true)) " -> " else ""
        val lemmaTag = lemma + ":" + tag

        val attrs = if(attr == null || attr.isEmpty) ""
                    else "(" + attr.map(node => node.toString).mkString(" ") + ")"

        start + lemmaTag + attrs + end
    }
}

