package biggi.util

import biggi.enhancer.clearnlp.FullClearNlpPipeline
import java.util.Properties
import biggi.model.AnnotatedText
import biggi.model.annotation.{Sentence}
import java.io.{FileInputStream}


/**
 * @author dirk
 * Date: 4/22/13
 * Time: 4:06 PM
 */
object PrintDependencyTree {

      def main(args:Array[String]) {
          val props = new Properties()
          props.load(new FileInputStream("conf/configuration.properties"))

          val pipeline = FullClearNlpPipeline.fromConfiguration(props)
          //val chunker = OpenNlpChunkEnhancer.fromConfiguration(props)
          //val qE =  new QuestionEnhancer(LuceneIndex.fromConfiguration(props,Version.LUCENE_36))

          var sentence = ""
          while(sentence != "a") {
              println("Write your sentence:")
              sentence = readLine()
              val text = new AnnotatedText(sentence)
              pipeline.enhance(text)
              //chunker.enhance(text)
              //UmlsEnhancer.enhance(text)
              //qE.enhance(text)


              text.getAnnotations[Sentence].foreach(s => {
                  println(s.getTokens.map(t => t.coveredText+"_"+t.posTag).mkString(" "))
                  println(s.dependencyTree.prettyPrint+"\n")
                  println(s.printRoleLabels)
                  //println(s.getAnnotationsWithin[OntologyEntityMention].map(_.toString).mkString("\t"))
                  //println(s.getAnnotationsWithin[Chunk].map(_.toString).mkString("\t"))

                  println()
              })
          }

      }

}
