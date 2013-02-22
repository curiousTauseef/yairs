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
class InvertedList(invertFile: File) extends Logging {
  private val lines = Source.fromFile(invertFile).getLines().toList

  val (term, stem, collectionFrequency, totalTermCount) = {
    val parts = lines(0).trim.split(" ")
    (parts(0), parts(1), parts(2).toInt, parts(3).toInt)
  }

  private var tempPostings = ListBuffer.empty[(Int, Int, Int, List[Int])]

  lines.slice(1, lines.length).foreach(posting => {
    val parts = posting.trim.split(" ")
    val Array(docId, tf, length) = parts.slice(0, 3).map(str => str.toInt)
    val positions = parts.slice(3, parts.length).map(str => str.toInt).toList
    tempPostings += ((docId,tf,length,positions))
  })

  val postings = tempPostings.toList

  def dump() {
    log.info(String.format("Dumping inverted list for [%s], with collection frequency [%s], total term count [%s]", term, collectionFrequency.toString, totalTermCount.toString))
    postings.foreach(posting =>
      println(String.format("[Doc id]: %s , [TF]: %s , [Document Length]: %s , %s positions are omitted", posting._1.toString,posting._2.toString,posting._3.toString,posting._4.length.toString))
    )
  }
}

object InvertedList extends Logging{
  def main(args: Array[String]) {
    log.info("Test Inverted List reading!")
    val ilr = new InvertedList(new File("data/clueweb09_wikipedia_15p_invLists/africa.inv"))
    ilr.dump()
    log.info("Done")
  }
}
