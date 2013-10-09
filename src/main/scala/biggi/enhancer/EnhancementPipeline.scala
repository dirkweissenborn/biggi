package biggi.enhancer

import clearnlp.FullClearNlpPipeline
import java.io.{FileWriter, File}
import collection.mutable._
import opennlp.OpenNlpChunkEnhancer
import org.apache.commons.logging.LogFactory
import java.util.Properties
import regex.RegexAcronymEnhancer
import biggi.model.AnnotatedText
import scala.concurrent.{Await, Future}
import biggi.io.AnnotatedTextSource.AnnotatedTextSource
import biggi.util.Xmlizer
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout


/**
 * @author dirk
 * Date: 4/18/13
 * Time: 10:49 AM
 */
class EnhancementPipeline(val enhancers:List[TextEnhancer]) {

    private implicit val timeout = Timeout(60 seconds)

    def this(enhancers:TextEnhancer*) = this(enhancers.toList)

    private val LOG = LogFactory.getLog(getClass)

    private var next = Map[TextEnhancer,TextEnhancer]()

    {
        var current:TextEnhancer = null
        val it = enhancers.iterator
        if(it.hasNext)
            current = it.next()
        else
            throw new IllegalArgumentException("Enhancement pipeline must consist of at least one enhancer")
        while(it.hasNext) {
            val nxt = it.next()
            next += (current -> nxt)
            current = nxt
        }
    }

    def process(texts:AnnotatedTextSource,outputDir:File = null, append:Boolean = true) {
        val writeOut = (outputDir != null)
        if (writeOut)
            outputDir.mkdirs()
        val assignedTexts = enhancers.foldLeft(Map[TextEnhancer,AnnotatedText]())( (acc,enhancer) => acc + (enhancer -> null))
        val futures = enhancers.foldLeft(Map[TextEnhancer,(AnnotatedText,Future[AnnotatedText])]())( (acc,enhancer) => acc + (enhancer -> null))

        var first:AnnotatedText = null

        if (texts.hasNext)
            first = texts.next()

        if (append && outputDir!=null) {
            val files = outputDir.list()
            while(texts.hasNext && files.contains(first.id+".xml") )
                first = texts.next()
        }

        if (first!=null) {
            assignedTexts(enhancers.head) = first

            while(assignedTexts.exists(_._2 != null)) {
                assignedTexts.foreach {
                    case (enhancer,text) => {
                        if (text != null) {
                            futures(enhancer) = (text,(enhancer.act ? text).mapTo[AnnotatedText])
                            assignedTexts(enhancer) = null
                        }
                    }
                }

                futures.foreach {
                    case (enhancer,future) => {
                        if (future != null) {
                            val text = Await.result(future._2, 60 seconds)

                            futures(enhancer) = null
                            next.get(enhancer) match {
                                case Some(nextEnhancer) => assignedTexts(nextEnhancer) = text
                                case None => {
                                    text.deleteDuplicates
                                    if (writeOut) {
                                        val fw = new FileWriter(new File(outputDir,text.id+".xml"))
                                        fw.write(Xmlizer.toXml(text))
                                        fw.close()
                                        LOG.info("Written result for input text: "+text.id)
                                    }
                                }
                            }
                        }
                    }
                }

                if (texts.hasNext)
                    assignedTexts(enhancers.head) = texts.next()
            }
        }
    }

    def processBatches(texts:AnnotatedTextSource, batchsize:Int, outputDir:File = null, append:Boolean = true) {
        val writeOut = (outputDir != null)
        if (writeOut)
            outputDir.mkdirs()
        val assignedTexts = enhancers.foldLeft(Map[TextEnhancer,List[AnnotatedText]]())( (acc,enhancer) => acc + (enhancer -> null))
        val futures = enhancers.foldLeft(Map[TextEnhancer,Future[List[AnnotatedText]]]())( (acc,enhancer) => acc + (enhancer -> null))

        var first = texts.take(batchsize).toList

        if (append && outputDir!=null) {
            val files = outputDir.list()
            first = first.dropWhile(t => files.contains(t.id+".xml"))
            while(texts.hasNext && first.isEmpty )
                first = texts.take(batchsize).dropWhile(t => files.contains(t.id+".xml")).toList
        }

        if (!first.isEmpty) {
            assignedTexts(enhancers.head) = first

            while(assignedTexts.exists(_._2 != null)) {
                assignedTexts.foreach {
                    case (enhancer,text) => {
                        if (text != null) {
                            futures(enhancer) = (enhancer.act ? text).mapTo[List[AnnotatedText]]
                            assignedTexts(enhancer) = null
                        }
                    }
                }

                futures.foreach {
                    case (enhancer,future) => {
                        if (future != null) {
                            val texts = Await.result(future, 60 seconds)
                            futures(enhancer) = null
                            next.get(enhancer) match {
                                case Some(nextEnhancer) => assignedTexts(nextEnhancer) = texts
                                case None => {
                                    if (writeOut) {
                                        texts.foreach(text => {
                                            val fw = new FileWriter(new File(outputDir,text.id+".xml"))
                                            fw.write(Xmlizer.toXml(text))
                                            fw.close()
                                            LOG.info("Written result for input text: "+text.id)
                                        })
                                    }
                                }
                            }
                        }
                    }
                }

                if (texts.hasNext)
                    assignedTexts(enhancers.head) = texts.take(batchsize).toList
            }
        }
    }
}

object EnhancementPipeline {
    def getFullPipeline(configuration: Properties, stanford: Boolean = false) = {
        val lexicalEnhancer = FullClearNlpPipeline.fromConfiguration(configuration)

        val chunker = OpenNlpChunkEnhancer.fromConfiguration(configuration)

        new EnhancementPipeline(List[TextEnhancer](lexicalEnhancer, chunker, RegexAcronymEnhancer, new OntologyEntitySelector(0.1))) //, new UniprotEnhancer, new DoidEnhancer, new GoEnhancer))//, new JochemEnhancer))
    }
}
