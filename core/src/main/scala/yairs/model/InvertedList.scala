package yairs.model

import org.eintr.loglady.Logging
import java.io.File
import io.Source
import collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/20/13
 * Time: 6:37 PM
 */
case class InvertedList(term: String, stem: String, collectionFrequency: Int, totalTermCount: Int, documentFrequency: Int, postings: List[Posting]) extends Logging {
  def dump() {
    log.info(String.format("Dumping inverted list for [%s], with collection frequency [%s], total term count[%s]", term, collectionFrequency.toString, totalTermCount.toString))
    var termCount = 0
    var lineCount = 0
    postings.foreach(posting => {
      termCount += posting.tf
      lineCount += 1
      println("[Doc id]: %s , [TF]: %s , [Document Length]: %s , %s positions are omitted".format(posting.docId, posting.tf, posting.docLength, posting.positions.length))
    })
    log.info(String.format("Dumped inverted list for [%s], with collection frequency [%s], total term count[%s]", term, collectionFrequency.toString, totalTermCount.toString))
    log.info("In this partial inverted list: Term count : [%s], Line count : [%s]".format(termCount, lineCount))
  }
}

object InvertedList extends Logging {
  /**
   * This main class is just to test whether reading is successful
   * @param args
   */
  def main(args: Array[String]) {
    log.info("Test Inverted List reading!")
    val ilr = InvertedList(new File("data/exp1/clueweb09_wikipedia_15p_invLists/africa.inv"))
    ilr.dump()
    log.info("Done")
  }


  def empty():InvertedList = {
    new InvertedList("","",0,0,0,List[Posting]())
  }

  def apply(collectionFrequency:Int, totalTermCount:Int, documentFrequency:Int, postings:List[Posting]):InvertedList = {
    new InvertedList("","",collectionFrequency,totalTermCount,documentFrequency,postings)
  }


  def apply(invertedFile: File, ranked: Boolean = true): InvertedList = {
    if (invertedFile.exists()) {
      val lines = Source.fromFile(invertedFile).getLines().toList
      val (term, stem, collectionFrequency, totalTermCount) = {
        val parts = lines(0).trim.split(" ")
        (parts(0), parts(1), parts(2).toInt, parts(3).toInt)
      }

      var tempPostings = ListBuffer.empty[Posting]

      lines.slice(1, lines.length).foreach(line => {
        val parts = line.trim.split(" ")
        val Array(docId, tf, length) = parts.slice(0, 3).map(str => str.toInt)
        val positions = parts.slice(3, parts.length).map(str => str.toInt).toList
        if (ranked)
          tempPostings += (new Posting(docId, tf, length, positions, tf))
        else
          tempPostings += (new Posting(docId, tf, length, positions, 1.0))
      })

      val postings = tempPostings.toList

      new InvertedList(term, stem, collectionFrequency, totalTermCount, postings.length, postings)
    } else {
      log.error("This inverted list is not found: " + invertedFile.getCanonicalPath)
      new InvertedList("", "", 0, 0, 0, List[Posting]())
    }
  }


  /**
   * Homework 1 inverted files does not have document frequency.
   * So a new apply method is used here with one more boolean
   * @param invertedFile
   * @param scorer
   * @return
   */
  def apply(invertedFile: File, scorer:(Int,Int,Int,Int)=>Double): InvertedList = {
    if (invertedFile.exists()) {
      val lines = Source.fromFile(invertedFile).getLines().toList
      val (term, stem, collectionFrequency, totalTermCount, documentFreq) = {
        val parts = lines(0).trim.split(" ")
        if (parts.length == 5)
          (parts(0), parts(1), parts(2).toInt, parts(3).toInt, parts(4).toInt)
        else
          (parts(0), parts(0), parts(1).toInt, parts(2).toInt, parts(3).toInt)//an problem in inverted list file
      }

      if (collectionFrequency < 0) {
        println(invertedFile.getCanonicalPath)
        println(collectionFrequency)
        System.exit(1)
      }

      var tempPostings = ListBuffer.empty[Posting]

      lines.slice(1, lines.length).foreach(line => {
        val parts = line.trim.split(" ")
        val Array(docId, tf, length) = parts.slice(0, 3).map(str => str.toInt)
        val positions = parts.slice(3, parts.length).map(str => str.toInt).toList

        tempPostings += (new Posting(docId, tf, length, positions, scorer(collectionFrequency,documentFreq,tf,length)))
      })

      val postings = tempPostings.toList

      new InvertedList(term, stem, collectionFrequency, totalTermCount, documentFreq, postings)
    } else {
      log.error("This inverted list is not found: " + invertedFile.getCanonicalPath)
      new InvertedList("", "", 0, 0, 0, List[Posting]())
    }
  }

}
