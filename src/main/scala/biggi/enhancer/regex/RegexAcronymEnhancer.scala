package biggi.enhancer.regex


import biggi.enhancer.TextEnhancer
import biggi.model._
import biggi.model.annotation._

/**
 * @author dirk
 * Date: 5/16/13
 * Time: 2:31 PM
 */
object RegexAcronymEnhancer extends TextEnhancer{
    private final val pattern = """([A-Z][A-Z0-9\-\&/]+)|([b-df-hj-np-tv-xzB-DF-HJ-NP-TV-XZ][b-df-hj-np-tv-xzB-DF-HJ-NP-TV-Z0-9\-\&/]{3,})"""

    protected def pEnhance(text: AnnotatedText) {
        text.getAnnotations[Token].filter(t => t.posTag.matches(PosTag.ANYNOUN_PATTERN) && t.coveredText.matches(pattern)).foreach(t => {
            if (!text.getAnnotations[OntologyEntityMention].exists(_.contains(t.begin,t.end)))
                new OntologyEntityMention(t.begin,t.end,t.context,
                    List(new OntologyConcept(OntologyConcept.SOURCE_ACRONYM,t.coveredText.toUpperCase,t.coveredText.toUpperCase,Set[String](),0.5)))
        })
    }
}
