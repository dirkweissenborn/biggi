package biggi.statistics

import java.io.File
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.util.Version
import org.apache.lucene.index._
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.search.{TermQuery, IndexSearcher}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.document.{StringField, Field, TextField, Document}

/**
 * @author dirk
 *          Date: 11/22/13
 *          Time: 4:28 PM
 */
object MergeCollectedCoocs {

    def main(args:Array[String]) {
        val INDEXES_DIR  = new File(args(0))
        val INDEX_DIR    = new File(args(1))

        INDEX_DIR.mkdir()

        try {
            val analyzer = new StandardAnalyzer(Version.LUCENE_44)
            val config = new IndexWriterConfig(Version.LUCENE_44,analyzer)
            config.setRAMBufferSizeMB(1024)
            val writer = new IndexWriter(FSDirectory.open(INDEX_DIR),config)
            writer.commit()

            val reader = new MultiReader(INDEXES_DIR.listFiles().map(file => DirectoryReader.open(FSDirectory.open(file))):_*)
            val searcher = new IndexSearcher(reader)

            val termEnum = SlowCompositeReaderWrapper.wrap(reader).terms("from-to").iterator(null)
            var counter = 0
            var terms = Set[String]()
            while (termEnum.next() != null)
                terms += termEnum.term().utf8ToString()

            println(terms.size+" pairs to process!")

            terms.foreach(fromTo => {
                val Array(from,to) = fromTo.split("-",2)
                val doc = new Document
                doc.add(new StringField("from-to", fromTo , Field.Store.YES))
                doc.add(new StringField("from", from , Field.Store.YES))
                doc.add(new StringField("to", to , Field.Store.YES))
                searcher.search(new TermQuery(new Term("from-to",fromTo)),10000).scoreDocs.foreach(sdoc => {
                    val fields: Array[IndexableField] = searcher.doc(sdoc.doc).getFields("relation")
                    fields.foreach(field => doc.add(field))
                })
                writer.addDocument(doc)
                counter += 1
                if(counter % 10000 == 0) {
                    println(s"$counter edges processed")
                    writer.commit()
                }
            })

            println("done")
            writer.close()

        } catch {
            case e:Throwable => e.printStackTrace()
        }
        System.exit(0)
    }

}
