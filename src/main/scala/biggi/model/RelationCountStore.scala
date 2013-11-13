package biggi.model

import java.io.File
import scala.io.Source
import scala.collection.concurrent.TrieMap

/**
 * @author dirk
 *          Date: 11/11/13
 *          Time: 10:37 AM
 */
class RelationCountStore {

    private var trie = TrieMap[String,Int]()
    private var totalCount = 0

    def getCount(rel:String) = {
        trie.get(rel).getOrElse(0)
    }
    
    def contains(rel:String) = {
        trie.contains(rel)
    }

    def update(rel:String, by:Int = 1) = {
        trie += rel -> getCount(rel)
    }

    def getTotalCount = totalCount
}

object RelationCountStore {
    def fromFile(pathsFile:File) = {
        val store = new RelationCountStore

        Source.fromFile(pathsFile).getLines().foreach(line => {
            val Array(rel,ct) = line.split("\t",2)
            val count = ct.toInt
            store.totalCount += count
            store.trie += rel -> count
        })
        store
    }
}
