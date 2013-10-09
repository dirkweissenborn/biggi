package biggi.enhancer.opennlp

import java.io.{FileInputStream, File}
import opennlp.tools.chunker.{Chunker, ChunkerME, ChunkerModel}
import org.apache.commons.logging.LogFactory
import java.util.Properties
import biggi.enhancer.TextEnhancer
import biggi.model.AnnotatedText
import biggi.model.annotation.{Chunk, Sentence}


/**
 * @author dirk
 * Date: 4/2/13
 * Time: 10:11 AM
 */
class OpenNlpChunkEnhancer(val modelFile:File) extends TextEnhancer{
    val LOG =  LogFactory.getLog(getClass)
    private var chunker:Chunker = null

    try {
        val fis = new FileInputStream(modelFile)
        val model = new ChunkerModel(fis)
        val chunkerModelAbsPath = modelFile.getAbsolutePath
        LOG.info("Chunker model file: " + chunkerModelAbsPath)
        chunker = new ChunkerME(model)
    }
    catch {
        case e: Exception => {
            LOG.error("Chunker model: " + modelFile.getAbsolutePath+" could not be initialized")
            throw new ExceptionInInitializerError(e)
        }
    }

    protected def pEnhance(text: AnnotatedText) {
        text.getAnnotations[Sentence].foreach(sentence => {
            val tokens = sentence.getTokens.toArray
            val tags = sentence.getTokens.map(_.posTag)

            val chunks = chunker.chunkAsSpans(tokens.map(_.coveredText).toArray,tags.toArray)
            if (chunks.size > 0) {
                val acc = chunks.sortBy(_.getStart).foldLeft(List[opennlp.tools.util.Span]())((acc,chunk) => {
                    if(acc.isEmpty) {
                        chunk :: acc
                    } else {
                        if(acc.head.getType.equals(chunk.getType)) {
                            chunk :: acc
                        }
                        else {
                            new Chunk(tokens(acc.last.getStart).begin,tokens(acc.head.getEnd-1).end,text,acc.head.getType)
                            List[opennlp.tools.util.Span](chunk)
                        }
                    }
                })
                new Chunk(tokens(acc.last.getStart).begin,tokens(acc.head.getEnd-1).end,text,acc.head.getType)
            }
            /*chunks.foreach(chunk => {
                try {
                    new Chunk(tokens(chunk.getStart).begin,tokens(chunk.getEnd-1).end,text,chunk.getType)
                } catch {
                    case e => e.printStackTrace()
                }
            }) */

        })
    }
}

object OpenNlpChunkEnhancer {
    var enhancer: OpenNlpChunkEnhancer = null
    val LOG =  LogFactory.getLog(getClass)

    final val MODEL_PROPERTY_NAME = "enhancer.opennlp.chunk.model"

    def fromConfiguration(properties:Properties):OpenNlpChunkEnhancer = {
        if (enhancer == null) {
            val modelPath = properties.getProperty(MODEL_PROPERTY_NAME)
            if (modelPath!=null)
                enhancer = new OpenNlpChunkEnhancer(new File(modelPath))
            else {
                LOG.error("Couldn't find opennlp chunker model! Please check your configuration for parameter "+MODEL_PROPERTY_NAME+"!")
                null
            }
        }
        enhancer
    }
}
