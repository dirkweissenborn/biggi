package biggi.index

import java.io.File
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.util.Version
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import biggi.util.BiggiFactory
import com.thinkaurelius.titan.core.TitanFactory
import scala.collection.JavaConversions._
import org.apache.lucene.document._
import org.apache.commons.logging.LogFactory

/**
 * @author dirk
 *          Date: 11/13/13
 *          Time: 10:17 AM
 */
object MovePMVerticesToLucene {

    private final val LOG = LogFactory.getLog(getClass)

    def main(args:Array[String]) {
        val graphDir = new File(args(0))
        val luceneDir = new File(args(1))

        val analyzer = new StandardAnalyzer(Version.LUCENE_44)
        val config = new IndexWriterConfig(Version.LUCENE_44,analyzer)
        config.setRAMBufferSizeMB(1024)

        val fSDirectory = FSDirectory.open(luceneDir)
        val indexWriter = new IndexWriter(fSDirectory, config)
        indexWriter.commit()

        val titanConf = BiggiFactory.getGraphConfiguration(graphDir)
        titanConf.setProperty("storage.transactions","false")
        //smallConf.setProperty("storage.read-only","true")
        val graph = TitanFactory.open(titanConf)

        var counter = 0
        graph.getVertices.foreach(vertex => {
            val ui = vertex.getProperty[String](BiggiFactory.UI)
            if(ui.contains(".")) {
                val Array(pmid,section,number) = ui.split("""\.""",3)
                val doc = new Document
                doc.add(new StringField("pmid", pmid , Field.Store.YES))
                doc.add(new StringField("section", section , Field.Store.YES))
                doc.add(new IntField("number", number.toInt , Field.Store.YES))
                doc.add(new TextField("text", vertex.getProperty[String](BiggiFactory.TEXT), Field.Store.YES))

                indexWriter.addDocument(doc)
                graph.removeVertex(vertex)

                counter += 1
                if(counter % 10000 == 0) {
                    indexWriter.commit()
                    graph.commit
                    LOG.info(s"$counter vertices removed & written to lucene!")
                }
            }
        })

        indexWriter.commit()
        graph.commit()
        indexWriter.close()
        graph.shutdown()
        LOG.info("DONE!")
        System.exit(0)
    }

}
