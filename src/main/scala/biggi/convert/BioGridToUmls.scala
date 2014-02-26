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
object BioGridToUmls {

     def main(args:Array[String]) {
         val mrConsoFile = new File("/ssd/data/umls/MRCONSO.RRF")
         val biogridFile = new File("/data/biogrid/BIOGRID-ALL-3.2.109.tab.txt")

         def normalizeName(name:String) = name.trim.toLowerCase.replaceAll("-"," ")

         val map = scala.collection.mutable.Map[String,mutable.HashSet[String]]()
         Source.fromFile(mrConsoFile).getLines().foreach(line => {
             val split = line.split("""\|""")
             val cui = split(0)
             val name = split(14)
             val lang = split(1)
             if(lang == "ENG" && split(6) == "Y")
                 map.getOrElseUpdate(normalizeName(name),mutable.HashSet[String]()) += cui
         })

         def mapNameToUmls(_name: String) = {
             val name = normalizeName(_name)
             val mapping = map.getOrElse(name, Set[String]())
             if (!mapping.isEmpty)
                 Some(mapping)
             else
                 None
         }

         val umlsMappings = scala.collection.mutable.Map[String,mutable.HashSet[String]]()
         var pw = new PrintWriter(new FileWriter("/ssd/data/protein_protein_biogrid.tsv"))

         Source.fromFile(biogridFile).getLines().drop(36).foreach(line => {
             //INTERACTOR_A INTERACTOR_B OFFICIAL_SYMBOL_A OFFICIAL_SYMBOL_B ALIASES_FOR_A ALIASES_FOR_B EXPERIMENTAL_SYSTEM SOURCE PUBMED_ID ORGANISM_A_ID ORGANISM_B_ID
             val split = line.split("\t")

             val officialA = split(2)
             val officialB = split(3)

             val aliasesA = officialA :: split(4).split("\\|").toList
             val aliasesB = officialB :: split(5).split("\\|").toList

             val mapsA = aliasesA.flatMap(mapNameToUmls).flatten
             val mapsB = aliasesB.flatMap(mapNameToUmls).flatten

             if(!mapsA.isEmpty && !mapsB.isEmpty) {
                 val mapA = mapsA.foldLeft(Map[String,Int]())((acc,el) => if(acc.contains(el))
                     acc + (el -> (acc(el) + 1))
                 else
                     acc + (el -> 1)).maxBy(_._2)._1

                 val mapB = mapsB.foldLeft(Map[String,Int]())((acc,el) => if(acc.contains(el))
                     acc + (el -> (acc(el) + 1))
                 else
                     acc + (el -> 1)).maxBy(_._2)._1

                 umlsMappings.getOrElseUpdate(officialA, mutable.HashSet[String]()) ++= mapsA
                 umlsMappings.getOrElseUpdate(officialB, mutable.HashSet[String]()) ++= mapsB

                 pw.println(mapA+"\t"+mapB)
             }
         })

         pw.close()

         pw = new PrintWriter(new FileWriter("/ssd/data/bioGridToUmls.tsv"))

         umlsMappings.foreach{
             case (biogridId,cuis) =>
                 pw.println(biogridId +" \t "+ cuis.mkString(","))
         }
         pw.close()
     }

 }
