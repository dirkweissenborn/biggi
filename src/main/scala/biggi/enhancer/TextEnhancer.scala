package biggi.enhancer

import biggi.model.AnnotatedText
import biggi.model.annotation.Annotation
import akka.actor.ActorDSL._
import biggi.util.BiggiActorSystem


/**
 * @author dirk
 * Date: 3/28/13
 * Time: 12:46 PM
 */
abstract class TextEnhancer {
    private val id = this.getClass.getSimpleName

    protected def pEnhance(text: AnnotatedText)

    def enhance(text: AnnotatedText) {
        if(!text.enhancedBy.contains(id)) {
            pEnhance(text)
            text.enhancedBy += id
        }
    }

    def enhanceBatch(texts:Seq[AnnotatedText]) {
        val batchText = new AnnotatedText(texts.map(_.text).mkString("\n"))
        var offset = 0
        texts.foreach(text => {
            val sortedAnnotations = text.getAllAnnotations.toSet[Annotation].toSeq.sortBy(_.begin)
            sortedAnnotations.foreach(an =>{
                //TODO CAREFUL with annotations which share spans
                an.spans.foreach(span => {
                    span.begin += offset
                    span.end += offset
                })
                an.context = batchText
                batchText.addAnnotation(an)
            })
            text.removeAllAnnotations
            offset += text.text.length+1
        } )

        enhance(batchText)

        offset = 0
        val iterator = texts.toIterator
        var text = iterator.next()
        val sortedAnnotations = batchText.getAllAnnotations.toSet[Annotation].toSeq.sortBy(_.end)
        sortedAnnotations.foreach(annotation => {
            while(offset + text.text.length < annotation.end) {
                offset += text.text.length+1
                text = iterator.next()
            }
            if (offset <= annotation.begin) {
                annotation.spans.foreach(span => {
                    span.begin -= offset
                    span.end -= offset
                })
                annotation.context = text

                text.addAnnotation(annotation)
            }
        })
    }

    val act = actor(BiggiActorSystem.instance)(new Act {
        become {
            case text:AnnotatedText => enhance(text); sender ! text
            case texts:List[AnnotatedText] => enhanceBatch(texts); sender ! texts
            case _ => sender ! null
        }
    })
}
