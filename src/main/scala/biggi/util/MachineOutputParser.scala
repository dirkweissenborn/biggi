package biggi.util

import scala.util.parsing.combinator._
import scala.io.Source
import org.apache.commons.logging.LogFactory


class MachineOutputParser extends RegexParsers {

    private final val LOG = LogFactory.getLog(this.getClass)

    val singleQuote:Parser[String] = "'" ~> """(\\'|[^']|''|'(?![,\[\]()]))*""".r <~ "'"
    val doubleQuote:Parser[String] = "\"" ~> """(\\"|[^"]|""|"(?![,\[\]()]))*""".r <~ "\""
    val symbol:Parser[String] = """[^,\n()\[\]]*""".r
    val number:Parser[String] = """-?\d+""".r
    val symbolOrQuote:Parser[String] = singleQuote | symbol

    def stringList:Parser[List[String]] = "[" ~> repsep(symbol,",") <~ "]"

    def list[T](elements:Parser[T]):Parser[List[T]] = "[" ~> repsep(elements,",") <~ "]"

    def utterance:Parser[MMUtterance] =
        rep("\n") ~> ("args" ~> """[^\n]+\n""".r).? ~>
        ("aas" ~> """[^\n]+\n""".r).? ~>
        ("neg_list" ~> """[^\n]+\n""".r).? ~>
        "utterance(" ~> singleQuote ~ "," ~ doubleQuote ~ "," ~ symbol ~ "," ~ stringList ~ ").\n" ~
        phrases <~ "'EOU'." <~ rep("\n") ^^ {
            case id ~ "," ~ text ~ "," ~ positionalInfo ~ "," ~ _ ~ _ ~ mmPhrases => {
                val Array(start,length) = positionalInfo.split("/",2)
                val Array(pmid,sec,num) = id.split("""\.""",3)
                MMUtterance(pmid,sec,num.toInt,text,start.toInt,length.toInt,mmPhrases)
            }
        }

    def phrases:Parser[List[MMPhrase]] = rep(phrase)

    def phrase:Parser[MMPhrase] =
        "phrase(" ~> symbolOrQuote ~> "," ~> """.*\],""".r ~> symbol ~ """,[^\n]+\n""".r ~ candidates ~ mappings ^^ {
            case positionalInfo ~ _ ~ cands ~ maps => {
                val Array(start,length) = positionalInfo.split("/",2)
                MMPhrase(start.toInt,length.toInt,cands,maps)
            }
        }

    def candidates:Parser[List[MMCandidate]] =
        "candidates(" ~> number ~> "," ~> number ~> "," ~> number ~> "," ~> number ~> "," ~>
        "[" ~> repsep(candidate,",") <~ "])."

    //ev(-856,'C0183413','SPECULA, OPHTHALMIC','SPECULA, OPHTHALMIC',[ophthalmic,specula],[medd],[[[2,2],[1,1],2],[[3,3],[2,2],1]],yes,no,['SPN'],[168/12],0)
    def candidate:Parser[MMCandidate] =
        "ev(" ~> number ~ ","  ~ singleQuote ~ "," ~ symbolOrQuote ~ "," ~ symbolOrQuote ~ "," ~ stringList ~ "," ~
        stringList ~ "," ~ list(list(list(number) | number)) ~ "," ~ """(yes|no),(yes|no)""".r ~ "," ~
        list(singleQuote) ~ "," ~ stringList <~ "," <~ number <~ ")" ^^ {
            case score ~ "," ~ cui ~ "," ~ umlsName ~ "," ~ preferredName ~ "," ~ _ ~ "," ~
                 semtypes ~ "," ~ _ ~ "," ~ _ ~ "," ~
                 sources ~ "," ~ positions => {
                val rPositions = positions.map(positionalInfo => {
                    val Array(start,length) = positionalInfo.split("/",2)
                    (start.toInt,length.toInt)
                })

                MMCandidate(score.toInt,cui,semtypes,rPositions)
            }

        }

    def mappings:Parser[List[MMMapping]] = if(withMappings){
        "mappings([" ~> repsep(mapping,",") <~ "])."
    }
    else {
        "mappings[^\n]+".r  ^^ {
            case _ => List[MMMapping]()
        }
    }


    //map(-871,[ev(-660,'C0184511','Improved','Improved',[improved],[qlco],[[[1,1],[1,1],0]],no,no,['MTH','SNOMEDCT','CHV'],[159/8],0),ev(-856,'C0183413','SPECULA, OPHTHALMIC','SPECULA, OPHTHALMIC',[ophthalmic,specula],[medd],[[[2,2],[1,1],2],[[3,3],[2,2],1]],yes,no,['SPN'],[168/12],0)])
    def mapping:Parser[MMMapping] =
        "map(" ~> number ~ "," ~ list(candidate) <~ ")" ^^ {
            case score ~_~ candidates => {
                MMMapping(score.toInt,candidates.map(_.cui))
            }
        }

    var withMappings = true
    def parse(s: String, withMappings: Boolean = true) = {
        this.withMappings = withMappings
        val utt = parseAll(utterance, s) match {
            case Success(xss, _) => xss
            case other => {
                LOG.warn("syntax error: " + other)
                null
            }
        }

        utt
    }

}

/*
case class MMArgs(args:String, options:List[String])

case class MMNegList(negs:List[String]) */

case class MMUtterance(pmid:String,section:String,num:Int,text:String,startPos:Int,length:Int,phrases:List[MMPhrase])

case class MMPhrase(start:Int,length:Int, candidates:List[MMCandidate], mappings:List[MMMapping])

case class MMCandidate(score:Int,cui:String, semtypes:List[String], positions:List[(Int,Int)])

case class MMMapping(score:Int,cuis:List[String])

object MachineOutputParser {
    def main(args:Array[String]) {
        val parser = new MachineOutputParser
        val input = Source.fromFile("shit").mkString

        val utt = parser.parse(input)
        utt
    }
}
