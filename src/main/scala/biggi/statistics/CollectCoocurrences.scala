package biggi.statistics

import java.io._
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.util.Version
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import biggi.util.BiggiUtils
import com.thinkaurelius.titan.core.TitanFactory
import scala.collection.JavaConversions._
import com.tinkerpop.blueprints.Direction
import org.apache.lucene.document.{TextField, Field, StringField, Document}

/**
 * @author dirk
 *          Date: 11/22/13
 *          Time: 3:29 PM
 */
object CollectCoocurrences {

    def main(args:Array[String]) {
        val output = new File(args(1))
        val graphsDir = new File(args(0))

        val analyzer = new StandardAnalyzer(Version.LUCENE_43)
        val config = new IndexWriterConfig(Version.LUCENE_43,analyzer)
        config.setRAMBufferSizeMB(1024)

        val fSDirectory = FSDirectory.open(output)
        val indexWriter = new IndexWriter(fSDirectory, config)
        indexWriter.commit()

        graphsDir.listFiles().foreach(graphDir => {
            val (vertices,g) = {
                val conf = BiggiUtils.getGraphConfiguration(graphDir)
                conf.setProperty("storage.transactions","false")
                conf.setProperty("storage.cache-percentage",1)

                //smallConf.setProperty("storage.read-only","true")
                val graph = TitanFactory.open(conf)

                if(graphDir.list().exists(_.endsWith("ui_id.bin"))) {
                    val cuiIdMap = BiggiUtils.getCuiToID(graphDir)
                    (cuiIdMap.iterator.map(id => (graph.getVertex(id._2),id._1)),graph)
                }
                else
                    (graph.getVertices.iterator().map(v => (v,v.getProperty[String](BiggiUtils.UI))),graph)
            }

            var counter = 0
            var processed = Set[String]()
            vertices.foreach{ case (vertex,cui) => {
                vertex.getEdges(Direction.BOTH).groupBy(edge => (edge.getVertex(Direction.OUT).getProperty[String](BiggiUtils.UI),edge.getVertex(Direction.IN).getProperty[String](BiggiUtils.UI)))
                    .withFilter(e => !processed.contains(e._1._1+"-"+ e._1._2)).foreach{
                    case ((from,to),edges) => {
                        val doc = new Document()
                        doc.add(new StringField("from-to", from+"-"+to , Field.Store.YES))
                        doc.add(new StringField("from", from , Field.Store.YES))
                        doc.add(new StringField("to", to , Field.Store.YES))
                        edges.foreach(e => {
                            if(e.getPropertyKeys.contains(BiggiUtils.LABEL))
                                doc.add(new StringField("relation",e.getProperty[String](BiggiUtils.LABEL), Field.Store.YES))
                            else
                                doc.add(new StringField("relation",e.getLabel, Field.Store.YES))
                        })
                        indexWriter.addDocument(doc)
                        processed += from+"-"+to

                        counter += 1
                        if(counter % 10000 == 0) {
                            println(s"$counter vertices processed")
                            indexWriter.commit()
                        }
                    }
                }
            } }
            g.shutdown()
        })

        indexWriter.close()
        println("Done")
        System.exit(0)
    }

}
