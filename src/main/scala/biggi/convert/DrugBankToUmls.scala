package biggi.convert

import java.io.{FileWriter, PrintWriter, File}
import scala.io.Source
import scala.collection.mutable
import cc.mallet.types.StringKernel
import org.apache.lucene.store.RAMDirectory
import org.apache.lucene.index.{Term, DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.search._
import org.apache.lucene.document.{StringField, TextField, Document}
import org.apache.lucene.document.Field.Store
import org.apache.lucene.search.BooleanClause.Occur

/**
 * Created by dirkw on 1/9/14.
 */
object DrugBankToUmls {

    def main(args:Array[String]) {
        val mrConsoFile = new File("/ssd/data/umls/MRCONSO.RRF")
        val drugBank = new File("/ssd/data/drugbank/drugbank.xml")
        val dbTargetFile = new File("/ssd/data/drugbank/approved_target_ids_all.csv")

        def normalizeName(name:String) = name.trim.toLowerCase.replaceAll("-"," ")

        val map = scala.collection.mutable.Map[String,mutable.HashSet[String]]()
        Source.fromFile(mrConsoFile).getLines().foreach(line => {
            val split = line.split("""\|""")
            val cui = split(0)
            val name = split(14)
            val lang = split(1)
            if(lang == "ENG")
                map.getOrElseUpdate(normalizeName(name),mutable.HashSet[String]()) += cui
        })

        val idx = new RAMDirectory()
        val conf = new IndexWriterConfig(Version.LUCENE_44, new WhitespaceAnalyzer(Version.LUCENE_44))
        val writer = new IndexWriter(idx, conf)


        map.foreach(entry => {
            val doc = new Document
            doc.add(new TextField("name",entry._1,Store.YES))
            entry._2.foreach(cui => {
                doc.add(new StringField("cui",cui,Store.YES))
            })
            writer.addDocument(doc)
        })

        writer.commit()
        writer.close()

        val reader = DirectoryReader.open(idx)
        val searcher = new IndexSearcher(reader)

        val sk = new StringKernel()
        def mapNameToUmls(_name: String) = {
            val name = normalizeName(_name)
            val mapping = map.getOrElse(name, Set[String]())
            if (!mapping.isEmpty)
                Some(mapping)
            else {
                println("Not found direct mapping for: " + name)
                val q = new BooleanQuery()
                name.split( """[ ,-;:()\[\]{}"']""").foreach(part => {
                    if (part.matches( """\d+"""))
                        q.add(new TermQuery(new Term("name", part)), Occur.MUST)
                    else
                        q.add(new TermQuery(new Term("name", part)), Occur.MUST)
                })
                val suggestions = searcher.search(q, 10).scoreDocs
                if (!suggestions.isEmpty) {
                    val suggestion = suggestions.map(s => reader.document(s.doc)).head//.maxBy(d => sk.K(name, d.get("name")))
                    val score: Double = sk.K(name, suggestion.get("name"))
                    if (score > 0.6) {
                        println(" -> using closest match, mapped to: " + suggestion.get("name"))
                        Some(suggestion.getValues("cui").toSet)
                    }
                    else
                        None
                }
                else None
            }
        }

        val drugMappings = scala.collection.mutable.Map[String,mutable.HashSet[String]]()

        var lastWasId = false
        var lastId = ""

        Source.fromFile(drugBank).getLines().drop(1).foreach(line => {
            if(lastWasId) {
                val name = line.substring(line.indexOf("<name>")+6, line.indexOf("</name>"))
                val maps = mapNameToUmls(name)
                if(maps.isDefined)
                    drugMappings.getOrElseUpdate(lastId, mutable.HashSet[String]()) ++= maps.get

                lastWasId = false
            }
            else {
                val idx = line.indexOf("<drugbank-id>")
                if(idx >= 0) {
                    lastId = line.substring(idx + 13, line.indexOf("</drugbank-id>"))
                    lastWasId = true
                }
            }
        })

        val targetMappings = scala.collection.mutable.Map[String,mutable.HashSet[String]]()

        var pw = new PrintWriter(new FileWriter("/ssd/data/drugTargetFromDB.tsv"))

        Source.fromFile(dbTargetFile).getLines().foreach(line => {
            //ID,Name,Gene Name,GenBank Protein ID,GenBank Gene ID,UniProt ID,Uniprot Title,PDB ID,GeneCard ID,GenAtlas ID,HGNC ID,HPRD ID,Species Category,Species,Drug IDs
            val split = line.split(",")
            val targetName = split(1).replaceAll("\"","")
            val drugIds = split.last
            val targetId = split(0)

            val maps = mapNameToUmls(targetName)
            if(maps.isDefined) {
                targetMappings.getOrElseUpdate(targetId, mutable.HashSet[String]()) ++= maps.get
                val drugs = drugIds.split(",").map(d => drugMappings.getOrElse(d, mutable.HashSet[String]())).reduce(_ ++ _)

                maps.get.foreach(targetCui => drugs.foreach(drugCui => {
                    pw.println(drugCui+"\t"+targetCui)
                }))
            }
        })
        pw.close()

        pw = new PrintWriter(new FileWriter("/ssd/data/dbDrugsToUmls.tsv"))

        drugMappings.foreach{
            case (dbId,cuis) =>
                pw.println(dbId +" \t "+ cuis.mkString(","))
        }
        pw.close()

        pw = new PrintWriter(new FileWriter("/ssd/data/dbTargetsToUmls.tsv"))

        targetMappings.foreach{
            case (dbId,cuis) =>
                pw.println(dbId +" \t "+ cuis.mkString(","))
        }
        pw.close()
    }

}
