package biggi.index

import java.io.File
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.util.Version
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory

/**
 * @author dirk
 *          Date: 11/13/13
 *          Time: 10:49 AM
 */
object MergeIndexes {

    def main(args:Array[String]) {
        val INDEXES_DIR  = new File(args(0))
        val INDEX_DIR    = new File(args(1))

        INDEX_DIR.mkdir()

        try {
            val analyzer = new StandardAnalyzer(Version.LUCENE_44)
            val config = new IndexWriterConfig(Version.LUCENE_44,analyzer)
            config.setRAMBufferSizeMB(1024)
            val writer = new IndexWriter(FSDirectory.open(INDEX_DIR),config)

            val indexes = INDEXES_DIR.listFiles().map(file => FSDirectory.open(file))

            System.out.print("Merging added indexes...")
            writer.addIndexes(indexes:_*)
            System.out.println("done")
            writer.close()

        } catch {
            case e => e.printStackTrace()
        }
    }

}
