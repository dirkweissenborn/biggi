package biggi.index

import org.apache.commons.logging.LogFactory
import java.io.{FileWriter, PrintWriter, FileInputStream, File}
import java.util.zip.GZIPInputStream
import biggi.util.{BiggiFactory, MMCandidate, MMUtterance, MachineOutputParser}
import scala.io.Source

import java.util.Properties
import biggi.enhancer.TextEnhancer
import biggi.enhancer.clearnlp.FullClearNlpPipeline
import com.thinkaurelius.titan.core.{TitanGraph, TitanFactory}
import biggi.model.{PosTag, AnnotatedText}
import biggi.model.annotation._
import com.tinkerpop.blueprints.{Vertex, Direction}
import scala.collection.JavaConversions._
import com.tinkerpop.blueprints.Query.Compare
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration.Duration
import java.util.concurrent.{TimeoutException, TimeUnit}
import biggi.model.annotation.DepTag
import scala.Some
import biggi.util.MMCandidate
import biggi.util.MMUtterance

/**
 * @author dirk
 *          Date: 8/21/13
 *          Time: 2:00 PM
 */
object IndexAsGraphFromMMBaseline {
    //ugly HACK
    class MyCustomExecutionContext extends AnyRef with ExecutionContext {
        @volatile var lastThread: Option[Thread] = None
        override def execute(runnable: Runnable): Unit = {
            ExecutionContext.Implicits.global.execute(new Runnable() {
                override def run() {
                    lastThread = Some(Thread.currentThread)
                    runnable.run()
                }
            })
        }
        override def reportFailure(t: Throwable): Unit = ???
    }

    implicit val exec = new MyCustomExecutionContext()


    private val LOG = LogFactory.getLog(getClass)

    private var mmoToXml = ""
    private var depParser:TextEnhancer = null

    private final var cuiIdMap =  Map[String,AnyRef]()


    def main(args:Array[String]) {

        val inputFile: String = args(0)
        val is = new GZIPInputStream(new FileInputStream(inputFile))
        val indexDir = new File(args(1))
        val _override = if(args.size > 3)
            args(3) == "override"
        else
            false

        val newGraph = indexDir.mkdirs() || _override || indexDir.list().isEmpty

        if(_override){
            LOG.info("Overriding output directory!")
            def deleteDir(dir:File) {
                dir.listFiles().foreach(f => {
                    if (f.isDirectory) {
                        deleteDir(f)
                        f.delete()
                    }
                    else
                        f.delete()
                })
            }
            deleteDir(indexDir)
        }

        val conf = new Properties()
        conf.load(new FileInputStream(new File(args(2))))

        depParser = FullClearNlpPipeline.fromConfiguration(conf, true)

        var currentUtterance = ""

        val parser = new MachineOutputParser

        //TITAN init
        val titanConf = BiggiFactory.getGraphConfiguration(indexDir)
        titanConf.setProperty("storage.buffer-size","2048")
        titanConf.setProperty("ids.block-size","500000")

        val graph = TitanFactory.open(titanConf)
        if(newGraph) {
            BiggiFactory.initGraph(graph)
        } else
            processedUtts = graph.getVertices(BiggiFactory.TYPE,"utterance").map(e => {
               e.getProperty[String](BiggiFactory.UI)
            }).toSet

        { //write configuration
            val pw = new PrintWriter(new FileWriter(new File(indexDir,"graph.config")))
            pw.println(BiggiFactory.printGraphConfiguration(indexDir))
            pw.close()
        }

        LOG.info(inputFile+": Processing: "+inputFile)

        var future:Future[Unit] = null

        var counter = 0
        val utteranceRegex = """utterance\('([^']+)',""".r


        Source.fromInputStream(is).getLines().foreach(line => {
            try {
                currentUtterance += line + "\n"

                if(line.equals("'EOU'.")) {
                    try {
                        val utterance = utteranceRegex.findFirstIn(currentUtterance).getOrElse("0.0.0")
                        val id = utterance.substring(utterance.indexOf('\'')+1,utterance.lastIndexOf('\''))

                        if(!processedUtts.contains(id)) {
                            val mmUtterance = parser.parse(currentUtterance,false)

                            if(future ne null)
                                try {
                                    Await.result(future,Duration(30,TimeUnit.SECONDS))
                                } catch {
                                    case timeout: TimeoutException =>
                                        (0 until 2).foreach { _ => exec.lastThread.get.stop() }
                                }
                            if(mmUtterance ne null) {
                                future = Future { extractConnections(mmUtterance,graph) }
                            }
                        }
                    }
                    catch {
                        case e:Exception => {
                            LOG.error(e.getMessage+ ", while parsing. Skipping it!")
                        }
                    }

                    counter += 1
                    currentUtterance = ""

                    if(counter % 1000 == 0) {
                        LOG.info(inputFile+": "+counter+" utterances processed!")
                        Await.result(future,Duration.Inf)
                        Await.result(future2,Duration.Inf)
                        graph.commit()
                    }
                }
            }
            catch {
                case e:Exception => {
                    LOG.error(inputFile+": "+e.printStackTrace())
                }
            }
        })
        Await.result(future,Duration.Inf)
        Await.result(future2,Duration.Inf)
        graph.shutdown()
        is.close()
        LOG.info("DONE!")
        System.exit(0)
    }

    var processedUtts = Set[String]()
    var future2: Future[Unit] =null
    def extractConnections(utt:MMUtterance, graph:TitanGraph) {
        val text = utt.text.replace("\"\"","\"") + "."
        val id = utt.pmid+"."+utt.section+"."+utt.num
        if(!processedUtts.contains(id)) {
            processedUtts += id

            val annotations = selectAnnotations(utt)

            if(annotations.size > 1) {
                try {
                    val annotatedText = new AnnotatedText(id,text)
                    depParser.enhance(annotatedText)

                    if(future2 ne null)
                        Await.result(future2,Duration.Inf)
                    future2 = Future{writeToGraph(annotatedText, text, annotations, graph)}
                }
                catch {
                    case e:Throwable => LOG.error(e.getMessage+"\nSkipping it!")
                }
            }
        }
    }


    def writeToGraph(annotatedText: AnnotatedText, text: String, annotations: Map[(Int, Int), List[MMCandidate]], graph: TitanGraph) {
        val id = annotatedText.id
        var offset = 0

        try {
            if(graph.query().has(BiggiFactory.UI, Compare.EQUAL, id).vertices().isEmpty) {
                val uttVertex = graph.addVertex(null)
                uttVertex.setProperty(BiggiFactory.UI,id)
                uttVertex.setProperty(BiggiFactory.TEXT,annotatedText.text)
                uttVertex.setProperty(BiggiFactory.TYPE,"utterance")

                annotations.toSeq.sortBy(_._1._1).foreach {
                    case ((start, end), candidates) => {
                        val entity = AnnotatedText.cleanText(text.substring(start, end))
                        val newStart = annotatedText.text.indexOf(entity, offset)
                        val newEnd = newStart + entity.length
                        offset = newStart + 1
                        val cands = candidates.filter(c => allowCandidate(c, entity))
                        if (cands.size > 0 &&
                            annotatedText.getAnnotationsBetween[Token](newStart, newEnd).exists(_.posTag.matches(PosTag.ANYNOUN_PATTERN)))
                            new OntologyEntityMention(
                                newStart,
                                newEnd,
                                annotatedText,
                                cands.map(c => new UmlsConcept(c.cui, c.preferredName, Set[String](), c.semtypes.toSet, Set[String](), c.score)))
                    }
                }

                annotatedText.getAnnotations[OntologyEntityMention].foreach(m1 => {
                    annotatedText.getAnnotations[OntologyEntityMention].foreach(m2 => {
                        if (m1.begin < m2.begin) {
                            val forbiddenAuxTokens = (m1.getTokens ++ m2.getTokens).toSet

                            normalizePath(getDepPath(annotatedText, m1, m2).getOrElse(List[Token]())) match {
                                case Some((path,reversed)) => {
                                    /*/deprecated: posttaged sequence between labels with window size 3
                                    val sentence = path.head._1.sentence
                                    val posTaggedSequence = sentence.getTokens.
                                        filter(t => t.position > m1.getTokens.last.position - 3 && t.position < m2.getTokens.head.position + 3 &&
                                                    !m1.getTokens.contains(t) && !m2.getTokens.contains(t)).
                                        map(t => {
                                            var result = printOnlyToken(t) +"_" +t.posTag
                                            if(t.position -1 == m1.getTokens.last.position)
                                                result = "$1 " + result
                                            if(t.position +1 == m2.getTokens.head.position)
                                                result += " $2"
                                            result
                                        }).mkString(" ").replaceAll("[\"´`]","'").replaceAll("}",")").replaceAll("""\{""","(")*/

                                    val rel = printPath(path,forbiddenAuxTokens)
                                    val (fromM,toM) = if(reversed) (m2,m1) else (m1,m2)

                                    fromM.ontologyConcepts.foreach(c1 => {
                                        toM.ontologyConcepts.foreach(c2 => {
                                            val fromId = cuiIdMap.get(c1.conceptId)
                                            val from =  fromId match {
                                                case Some(id) =>  graph.getVertex(id)
                                                case None => addConceptAsVertex(graph, c1)
                                            }

                                            val toId = cuiIdMap.get(c2.conceptId)
                                            val to =  toId match {
                                                case Some(id) =>  graph.getVertex(id)
                                                case None => addConceptAsVertex(graph, c2)
                                            }

                                            def addEdge(relation:String) {
                                                if(relation.length > 0)
                                                    from.getEdges(Direction.OUT, relation).iterator().find(_.getVertex(Direction.IN) == to) match {
                                                        case Some(edge) => {
                                                            val ids = edge.getProperty[String](BiggiFactory.SOURCE).split(",")
                                                            if (!ids.contains(id)) {
                                                                edge.setProperty(BiggiFactory.SOURCE, ids.mkString(",") + "," + id)
                                                            }
                                                        }
                                                        case None => {
                                                            val edge = graph.addEdge(null, from, to, relation)
                                                            edge.setProperty(BiggiFactory.SOURCE, id)
                                                        }
                                                    }
                                            }

                                            addEdge(rel)
                                            //addEdge(posTaggedSequence)
                                        })
                                    })
                                }
                                case None =>  //nothing to do
                            }

                        }
                    })
                })
            }
        }
        catch {
            case e: Exception => LOG.error("Skipping error: "+e.getMessage)
        }
    }


    private def addConceptAsVertex(graph: TitanGraph, c: OntologyConcept): Vertex = {
        val v = graph.addVertex(null)
        v.setProperty(BiggiFactory.UI, c.conceptId)
        v.setProperty(BiggiFactory.TYPE, c.asInstanceOf[UmlsConcept].semanticTypes.filter(s => selectedSemtypes.contains(s)).mkString(","))
        v.setProperty(BiggiFactory.TEXT, c.asInstanceOf[UmlsConcept].preferredLabel)
        cuiIdMap += c.conceptId -> v.getId
        v
    }

    private val selectedSemtypes =
        List("amph","anim","arch","bact","bird","euka","fish","fngs","humn","mamm","plnt","rept","vtbt","virs", //Organisms
            "acab","anab","bpoc","celc","cell","cgab","emst","ffas","gngm","tisu",  //anatomical structures
            "clnd",  //clinical drug
            "sbst","aapp","antb","bacs","bodm","bdsu","carb","chvf","chvs","chem","eico","elii","enzy","food", //Substances
            "hops","horm","imft","irda","inch","lipd","nsba","nnon","orch","opco","phsu","rcpt","strd","vita", //Substances
            "sosy",// Sign or Symptom
            "amas","blor","bsoj","crbs","mosq","nusq", //molecular sequences, body parts
            "patf","comd","dsyn","emod","mobd","neop",     //pathologic functions
            "inpo") //injury or poisening

    private val stopSemTypes = Set("ftcn","qlco","qnco","idcn","inpr")

    def allowCandidate(cand : MMCandidate, coveredText:String):Boolean = {
        val interSemTypes = cand.semtypes.intersect(selectedSemtypes)
        if(interSemTypes.size>0) {
            if(coveredText.length < 2)
                false
            else if(interSemTypes.contains("gngm") && interSemTypes.size < 2) {
                //some really common words are annotated as genes, other normal words are proteins.
                //Genes usually don't look like normal english words
                if(coveredText.matches("""[A-Z]?[a-z]+""") || coveredText.toLowerCase == "ii") {
                    false
                }
                else true
            } else if(coveredText.startsWith("level"))
                false
            else
                true
        }
        else false
    }

    //Start,End -> best annotation
    def selectAnnotations(utt:MMUtterance): Map[(Int,Int),List[MMCandidate]] = {
        val offset = utt.startPos
        val docLength = utt.length

        //position -> highest scored candidates
        var groupedAnnotations = utt.phrases.flatMap(phrase => {
            if(phrase.candidates.isEmpty)
               Map[(Int,Int),List[MMCandidate]]()
            else {
               phrase.candidates.flatMap(cand => cand.positions.map(pos => ((pos._1,pos._2), cand))).
                   groupBy(_._1).mapValues(cands => {
                   //metamap uses negative scores
                   val minScore = cands.minBy(_._2.score)._2.score
                   cands.filter(_._2.score == minScore).map(_._2)
               })
            }
        }).toMap

        //filter out overlapping annotations
        val filteredKeys = groupedAnnotations.keys.toList.sortBy(_._1).foldLeft(List[(Int,Int)]())((keys,key) => {
            if(keys.isEmpty)
                List(key)
            else {
                val last = keys.head
                if(last._1+last._2 < key._1)
                    key :: keys
                // if overlapping, take the one with the biggest length
                else if(key._2 <= last._2)
                    keys
                else
                    key :: keys.tail
            }
        })

        groupedAnnotations = groupedAnnotations.filter(ann => filteredKeys.contains(ann._1))

        // select annotations with highest (negative scores so actually lowest) scores with preference for preferred cuis
        groupedAnnotations.map {
            case ((start,length),annots) => {
                val newStart = start - offset
                val newEnd = newStart + length
                val bestAnnots =  annots.tail.foldLeft(List(annots.head))((bestPartAnnots,annot) => {
                    val max = bestPartAnnots.head.score
                    val current = annot.score
                    if(current < max)
                        List(annot)
                    else if(current > max)
                        bestPartAnnots
                    else
                        annot :: bestPartAnnots
                })

                if(newStart >= 0 && newEnd > newStart && newEnd < docLength && !bestAnnots.isEmpty)
                    ((newStart,newEnd), bestAnnots)
                else
                    ((newStart,newEnd), null)
            }
        }.filter(_._2 ne null)
    }

    def getDepPath(text: AnnotatedText, fromMention:OntologyEntityMention, toMention:OntologyEntityMention):Option[List[Token]] = {
        val startToken = fromMention.getTokens.minBy(_.depDepth)
        val endToken = toMention.getTokens.minBy(_.depDepth)

        if (startToken.sentence == endToken.sentence) {
            var token = startToken

            var path1 = List[Token](startToken)
            while (token.depDepth > 0 && token.depTag.dependsOn > 0 && token != endToken) {
                token = token.sentence.getTokens(token.depTag.dependsOn - 1)
                path1 ::= token
            }
            path1 = path1.reverse

            token = endToken
            var path2 = List[Token](endToken)
            while (token.depDepth > 0 && token.depTag.dependsOn > 0 && !path1.contains(token)) {
                token = token.sentence.getTokens(token.depTag.dependsOn - 1)
                path2 ::= token
            }

            if(path1.contains(token)) {
                //Combine paths and remove tokens which are part of the mentions
                val path = path1.takeWhile(t => t != token && !toMention.getTokens.contains(t)) ++ List(path2.head) ++ path2.tail.dropWhile(t => fromMention.getTokens.contains(t))
                Some(path)
            }
            else
                None
        }
        else
            None
    }

    private final val replaceSemtypes = Set("spco","phob","amas","blor","bsoj","crbs","geoa","mosq","nusq","acab","aapp","amph",
        "anab","anst","anim","antb","arch","bact","bacs","bodm","bird","bpoc","bdsu",
        "carb","celc","cell","chvf","chvs","chem","clnd","cgab","drdd","eico","elii",
        "emst","enzy","euka","fish","food","ffas","fngs","gngm","hops","horm","humn",
        "imft","irda","inch","lipd","mamm","mnob","medd","nsba","nnon","orch","orgm",
        "opco","phsu","plnt","rcpt","rept","resd","strd","sbst","tisu","vtbt","virs","vita","sosy","dsyn")

    def normalizePath(path: List[Token]):Option[(List[(Token,DepTag)], Boolean)] = {
        if(!path.isEmpty){
            var resultPath = path.map(t => (t,DepTag(t.depTag.tag,t.depTag.dependsOn)))

            //just consider paths in which at least one verbal form occurs
            if(resultPath.exists(_._1.posTag.matches(PosTag.ANYVERB_PATTERN))) {
                //replace tokens of spatial or physical mentions with there respective semantic type
               /* resultPath.head._1.context.getAnnotations[OntologyEntityMention].foreach(mention => {
                    val concept = mention.ontologyConcepts.head.asInstanceOf[UmlsConcept]
                    if(concept.semanticTypes.forall(selectedSemtypes.contains))
                        mention.getTokens.foreach(t => {
                            if(t.posTag.matches(PosTag.ANYNOUN_PATTERN))
                                t.lemma = concept.semanticTypes.head
                        })
                }) */

                //if passive, make it active
                if(resultPath.exists(_._2.tag == "nsubjpass")) {
                    val (_,depTag) = resultPath.find(_._2.tag == "nsubjpass").get
                    depTag.tag = "dobj"
                }

                if(resultPath.exists(_._2.tag == "agent")) {
                    val (byToken,byTag) = resultPath.find(_._2.tag == "agent").get
                    val passObjToken = resultPath.find(_._2.dependsOn == byToken.position)
                    val verbToken = resultPath.find(_._1.position == byTag.dependsOn)
                    if(verbToken.isDefined) {
                        resultPath.find{ case (t,depTag) => depTag.dependsOn == verbToken.get._1.position && depTag.tag == "nsubj" } match {
                            case Some((_,depTag)) => depTag.tag = "dobj"
                            case None =>
                        }
                    }
                    if(passObjToken.isDefined) {
                        val (_,passObjDepTag) = passObjToken.get
                        if(verbToken.isDefined) {
                            passObjDepTag.tag = "nsubj"
                            passObjDepTag.dependsOn = byTag.dependsOn
                        }
                        resultPath = resultPath.filter(_._1 != byToken)
                    }
                }

                //Clean conj and appos sequences
                val startToken = resultPath.head
                val endToken = resultPath.last

                def removeSequences(depLabel:String,tmpPath:List[(Token,DepTag)]): List[(Token,DepTag)] = {
                    tmpPath.tail.foldLeft(List[(Token,DepTag)](tmpPath.head)){
                        case (acc, (token,depTag)) => {
                            val (last,lastDepTag) = acc.head
                            if (depTag.tag == depLabel) {
                                if (token.depDepth > last.depDepth) {
                                    //downwards in tree
                                    depTag.tag = lastDepTag.tag
                                    depTag.dependsOn = lastDepTag.dependsOn
                                    (token,depTag) :: acc.tail
                                }
                                else //upwards
                                if (lastDepTag.tag == depLabel) //there is a deeper token of this depTag, so skip
                                    acc
                                else //deepest of token of this depTag
                                    (token,depTag) :: acc
                            } else if (lastDepTag.tag == depLabel) {
                                //can only happen upwards -> change depTag
                                lastDepTag.tag = depTag.tag
                                lastDepTag.dependsOn = depTag.dependsOn
                                acc
                            } else
                                (token,depTag) :: acc
                        }
                    }.reverse
                }

                resultPath = removeSequences("conj",resultPath)
                resultPath = removeSequences("appos",resultPath)

                //filter
                resultPath = resultPath.filterNot(_._2.tag.matches("""punct|hyph"""))

                //normalize
                val reversed = resultPath.head._2.tag > resultPath.last._2.tag
                if(reversed)
                    resultPath = resultPath.reverse

                if(resultPath.isEmpty || resultPath.head != startToken || resultPath.last != endToken ||
                    !resultPath.exists(_._1.posTag.matches(PosTag.ANYVERB_PATTERN)) || resultPath.size > 6)
                    None
                else
                    Some(resultPath,reversed)
            }
            else
                None
        }
        else None
    }


    def printOnlyToken(token: Token): String = {
        val result = if (token.posTag == PosTag.Cardinal_number) token.coveredText else token.lemma
        result.replaceAll("""["'´`]""","")
    }

    def printPath(path: List[(Token,DepTag)], forbiddenAuxTokens:Set[Token]) = {
        def printToken(token:Token)(deptag:DepTag = token.depTag):String = {
            var result = printOnlyToken(token)

            var auxTokens:List[Token] =
                if(token.posTag.matches(PosTag.ANYVERB_PATTERN)) {
                    if(token.posTag == PosTag.Verb_gerund_or_present_participle)
                        result += ":VBG"
                    else
                        result += ":VB"
                    token.sentence.getTokens.filter(t => t.depTag.dependsOn == token.position && (t.depTag.tag.matches("neg|acomp|oprd|aux") || t.lemma == "no"))

                } else if(token.posTag.matches(PosTag.ANYNOUN_PATTERN)) {
                    result += ":" + deptag.tag
                    token.sentence.getTokens
                        .filter(t => t.depTag.dependsOn == token.position && (t.depTag.tag.matches("amod|nn|neg") || t.lemma == "no"))
                } else if(token.coveredText == "%") {
                    token.sentence.getTokens
                        .filter(t => t.depTag.dependsOn == token.position && t.depTag.tag.matches("num"))
                } else List[Token]()

            auxTokens = auxTokens.filter(t => !path.exists(_._1 == t) && !forbiddenAuxTokens.contains(t))

            if(auxTokens.size > 0)
                result += "("+auxTokens.map(t => printToken(t)()).mkString(" ")+")"

            result
        }
        val startToken = path.head
        val endToken = path.last

        var result = { if(startToken._2.tag == "prep") "pobj -> "+printToken(startToken._1)() else startToken._2.tag }
        var last = startToken._1
        path.tail.dropRight(1).foreach(t => {
            if (last.depDepth < t._1.depDepth)
                result += " <- "
            else
                result += " -> "

            result += printToken(t._1)(t._2)
            last = t._1
        })
        if (last.depDepth < endToken._1.depDepth)
            result += " <- "
        else
            result += " -> "
        result += { if(endToken._2.tag == "prep") printToken(endToken._1)()+" <- pobj" else endToken._2.tag }

        result
    }
}
