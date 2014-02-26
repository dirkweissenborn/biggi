package biggi.index

import org.apache.commons.logging.LogFactory
import java.io.{FileWriter, PrintWriter, FileInputStream, File}
import java.util.zip.GZIPInputStream
import biggi.util.{BiggiUtils,MachineOutputParser}
import scala.io.Source

import java.util.Properties
import biggi.enhancer.TextEnhancer
import biggi.enhancer.clearnlp.FullClearNlpPipeline
import com.thinkaurelius.titan.core.{TitanGraph, TitanFactory}
import biggi.model.{PosTag, AnnotatedText}
import biggi.model.annotation._
import com.tinkerpop.blueprints.{Vertex, Direction}
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration.Duration
import java.util.concurrent.{TimeoutException, TimeUnit}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.util.Version
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.document._
import biggi.model.annotation.DepTag
import scala.Some
import biggi.util.MMCandidate
import biggi.util.MMUtterance
import scala.collection.JavaConversions._
import scala.collection.mutable

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

    private var depParser:TextEnhancer = null

    private final var cuiIdMap = mutable.Map[String,java.lang.Long]()

    private var indexWriter:IndexWriter = null

    def main(args:Array[String]) {
        val inputFile: String = args(0)
        val is = new GZIPInputStream(new FileInputStream(inputFile))
        val graphDir = new File(args(1))
        val indexDir = new File(graphDir,"pm_index")
        val _override = if(args.size > 3)
            args(3) == "override"
        else
            false

        val analyzer = new StandardAnalyzer(Version.LUCENE_44)
        val config = new IndexWriterConfig(Version.LUCENE_44,analyzer)
        config.setRAMBufferSizeMB(1024)

        val fSDirectory = FSDirectory.open(indexDir)
        indexWriter = new IndexWriter(fSDirectory, config)
        indexWriter.commit()

        val newGraph = graphDir.mkdirs() || _override || graphDir.list().isEmpty

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
            deleteDir(graphDir)
        }

        val conf = new Properties()
        conf.load(new FileInputStream(new File(args(2))))

        depParser = FullClearNlpPipeline.fromConfiguration(conf, true)

        var currentUtterance = ""

        val parser = new MachineOutputParser

        //TITAN init
        val titanConf = BiggiUtils.getGraphConfiguration(graphDir)
        titanConf.setProperty("storage.buffer-size","2048")
        titanConf.setProperty("ids.block-size","500000")
        titanConf.setProperty("storage.batch-loading",true)

        val graph = TitanFactory.open(titanConf)
        if(newGraph) {
            BiggiUtils.initGraph(graph)
        }

        { //write configuration
            val pw = new PrintWriter(new FileWriter(new File(graphDir,"graph.config")))
            pw.println(BiggiUtils.printGraphConfiguration(graphDir))
            pw.close()
        }

        LOG.info(inputFile+": Processing: "+inputFile)

        var future:Future[Unit] = null

        var counter = 0
        val utteranceRegex = """utterance\('([^']+)',""".r

        var processedUtts = Set[String]()

        Source.fromInputStream(is).getLines().foreach(line => {
            try {
                currentUtterance += line + "\n"

                if(line.equals("'EOU'.")) {
                    try {
                        val utterance = utteranceRegex.findFirstIn(currentUtterance).getOrElse("0.0.0")
                        val id = utterance.substring(utterance.indexOf('\'')+1,utterance.lastIndexOf('\''))

                        if(!processedUtts.contains(id)) {
                            processedUtts += id
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
                        case e:Exception =>
                            LOG.error(e.getMessage+ ", while parsing. Skipping it!")
                    }

                    counter += 1
                    currentUtterance = ""

                    if(counter % 1000 == 0) {
                        LOG.info(inputFile+": "+counter+" utterances processed!")
                        Await.result(future,Duration.Inf)
                        Await.result(future2,Duration.Inf)
                        indexWriter.commit()
                        graph.commit()
                    }
                }
            }
            catch {
                case e:Exception =>
                    LOG.error(inputFile+": "+e.printStackTrace())
            }
        })
        Await.result(future,Duration.Inf)
        Await.result(future2,Duration.Inf)
        graph.shutdown()
        indexWriter.commit()
        indexWriter.close()
        BiggiUtils.saveCuiToID(graphDir,mapAsJavaMap(cuiIdMap))

        is.close()
        LOG.info("DONE!")
        System.exit(0)
    }

    var future2: Future[Unit] =null
    def extractConnections(utt:MMUtterance, graph:TitanGraph) {
        val text = utt.text.replace("\"\"","\"") + "."
        val id = utt.pmid+"."+utt.section+"."+utt.num
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


    def writeToGraph(annotatedText: AnnotatedText, text: String, annotations: Map[(Int, Int), List[MMCandidate]], graph: TitanGraph) {
        val id = annotatedText.id
        var offset = 0

        try {
            val Array(pmid,section,number) = id.split("""\.""",3)
            val doc = new Document
            doc.add(new StringField("pmid", pmid , Field.Store.YES))
            doc.add(new StringField("section", section , Field.Store.YES))
            doc.add(new IntField("number", number.toInt , Field.Store.YES))
            doc.add(new TextField("text", annotatedText.text, Field.Store.YES))
            indexWriter.addDocument(doc)

            annotations.toSeq.sortBy(_._1._1).foreach {
                case ((start, end), candidates) =>
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

            var mentions = annotatedText.getAnnotations[OntologyEntityMention]
            mentions = cleanMentions(mentions)
            mentions.foreach(m1 => {
                mentions.foreach(m2 => {
                    if (m1.begin < m2.begin) {
                        val forbiddenAuxTokens = (m1.getTokens ++ m2.getTokens).toSet

                        normalizePath(getDepPath(annotatedText, m1, m2).getOrElse(List[Token]())) match {
                            case Some((path,reversed)) =>
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
                                                from.getEdges(Direction.OUT, BiggiUtils.EDGE).
                                                find(e => e.getVertex(Direction.IN) == to && e.getProperty[String](BiggiUtils.LABEL) == rel) match {
                                                    case Some(edge) =>
                                                        val ids = edge.getProperty[String](BiggiUtils.SOURCE).split(",")
                                                        if (!ids.contains(id)) {
                                                            edge.setProperty(BiggiUtils.SOURCE, ids.mkString(",") + "," + id)
                                                        }
                                                    case None =>
                                                        val edge = graph.addEdge(null, from, to, BiggiUtils.EDGE)
                                                        edge.setProperty(BiggiUtils.SOURCE, id)
                                                        edge.setProperty(BiggiUtils.LABEL, rel)
                                                }
                                        }

                                        addEdge(rel)
                                        //addEdge(posTaggedSequence)
                                    })
                                })
                            case None =>  //nothing to do
                        }

                    }
                })
            })
        }
        catch {
            case e: Exception => LOG.error("Skipping error: "+e.getMessage)
        }
    }


    def cleanMentions(_mentions: List[OntologyEntityMention]) = {
        if(!_mentions.isEmpty) {
            var mentions: List[OntologyEntityMention] = _mentions.sortBy(_.begin)
            mentions = mentions.filterNot(m => mentions.exists(m2 => m2.begin <= m.begin && m2.end >= m.end && m2.end - m2.begin > m.end - m.begin))
                               .filter(_.getTokens.exists(_.posTag.matches(PosTag.ANYNOUN_PATTERN)))

            val tokens = mentions.head.getTokens.head.sentence.getTokens

            mentions.tail.foldLeft(List[OntologyEntityMention](mentions.head)) {
                case (acc, secondM)  =>
                    val firstM = acc.head
                    val interTokens = tokens.filter(t => t.begin > firstM.end && t.end < secondM.begin)
                    if (interTokens.forall(t => t.posTag.matches(PosTag.Determiner+"|"+PosTag.Preposition_or_subordinating_conjunction))) {
                        val intersection = firstM.ontologyConcepts.intersect(secondM.ontologyConcepts)
                        if (!intersection.isEmpty)
                            new OntologyEntityMention(firstM.begin, secondM.end, firstM.context, intersection) :: acc.tail
                        else secondM :: acc
                    }
                    else secondM ::acc
            }.reverse
        } else _mentions
    }

    private def addConceptAsVertex(graph: TitanGraph, c: OntologyConcept): Vertex = {
        val v = graph.addVertex(null)
        v.setProperty(BiggiUtils.UI, c.conceptId)
        v.setProperty(BiggiUtils.TYPE, c.asInstanceOf[UmlsConcept].semanticTypes.filter(s => selectedSemtypes.contains(s)).mkString(","))
        v.setProperty(BiggiUtils.TEXT, c.asInstanceOf[UmlsConcept].preferredLabel)
        cuiIdMap += c.conceptId -> v.getId.asInstanceOf[java.lang.Long]
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
            case ((start,length),annots) =>
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
                val minDepth = path.map(_.depDepth).min
                //check that path doesn't contain verbs of different parts of the sentence
                if(!path.exists(t => (t.depTag.tag.matches("advcl|ccomp|csubj|csubjpass") || (t.depTag.tag == "conj" && t.posTag.matches(PosTag.ANYVERB_PATTERN))) && t.depDepth > minDepth))
                    Some(path)
                else
                    None
            }
            else
                None
        }
        else
            None
    }

    def normalizePath(path: List[Token]):Option[(List[(Token,DepTag)], Boolean)] = {
        if(!path.isEmpty){
            var resultPath = path.map(t => (t,t.depTag.copy))

            //just consider paths with at least one verbal form
            if(resultPath.exists(_._1.posTag.matches(PosTag.ANYVERB_PATTERN))) {
                //Clean conj and appos sequences
                val startToken = resultPath.head
                val endToken = resultPath.last

                val root = resultPath.minBy(_._1.depDepth)

                def removeSequences(depLabel:String,tmpPath:List[(Token,DepTag)]): List[(Token,DepTag)] = {
                    while(root._2.tag == depLabel) {
                        val newDeptag = root._1.sentence.getTokens.find(_.position == root._2.dependsOn).get.depTag
                        root._2.tag = newDeptag.tag; root._2.dependsOn = newDeptag.dependsOn
                    }
                    tmpPath.tail.foldLeft(List[(Token,DepTag)](tmpPath.head)){
                        case (acc, (token,depTag)) =>
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
                                else //deepest token of this depTag
                                    (token,depTag) :: acc
                            } else if (lastDepTag.tag == depLabel && token.depDepth < last.depDepth) {
                                //can only happen upwards -> change depTag
                                lastDepTag.tag = depTag.tag
                                lastDepTag.dependsOn = depTag.dependsOn
                                acc
                            } else
                                (token,depTag) :: acc
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
        var result = if (token.posTag == PosTag.Cardinal_number) token.coveredText else token.lemma
        result = result.replaceAll("""["'´`{}()]""","")
        result
    }

    def printPath(_path: List[(Token,DepTag)], forbiddenAuxTokens:Set[Token]) = {
        //HACK because of st**** metamap
        var path = _path
        
        {
            val head = path.head
            if(head._2.tag == "prep")
                path = head._1.sentence.getTokens.find(t => t.depTag.dependsOn == head._1.position).map(t => (t,t.depTag)).get :: path
            val last = path.last
            if(last._2.tag == "prep")
                path ++= List(last._1.sentence.getTokens.find(t => t.depTag.dependsOn == last._1.position).map(t => (t,t.depTag)).get)
        }
        
        val minDepth = path.map(_._1.depDepth).min
        def printToken(token:Token)(depTag:DepTag = token.depTag):String = {
            var result = printOnlyToken(token)
            if(token.depDepth > minDepth)
                result += ":"+depTag.tag

            var auxTokens:List[Token] =
                token.sentence.getTokens.filter(t => t.depTag.dependsOn == token.position && (t.depTag.tag.matches("neg|acomp|oprd|aux|auxpass|.*mod|num|nn") || t.lemma == "no"))

            auxTokens = auxTokens.filter(t => !path.exists(_._1 == t) && !forbiddenAuxTokens.contains(t))

            if(auxTokens.size > 0)
                result += "("+auxTokens.map(t => printToken(t)()).mkString(" ")+")"

            result
        }
        val startToken = path.head
        val endToken = path.last

        var result =printToken(startToken._1)(startToken._2).substring(printOnlyToken(startToken._1).length)
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
        //clean up, sometimes pubmed also annotates the prepositions
        result += printToken(endToken._1)(endToken._2).substring(printOnlyToken(endToken._1).length)

        result
    }
}
