package biggi.model.annotation

import biggi.model.{AnnotatedText, Span}


/**
 * @author dirk
 *          Date: 4/16/13
 *          Time: 3:24 PM
 */
class SemanticRelationAnnotation(val relationName:String, val subj:OntologyEntityMention, val obj:OntologyEntityMention) extends Annotation((subj.spans ++ obj.spans),subj.context) {
}
