package yairs.model

/**
 * Created with IntelliJ IDEA.
 * User: Hector
 * Date: 2/21/13
 * Time: 1:16 AM
 * To change this template use File | Settings | File Templates.
 */
class TrecLikeResult(val queryID: String, val docID: Int, val rank: Int, val score: Double, val runID: String) extends Result {
  override def toString = ("%s\tQ0%s\t%s\t%s\t%s").format(queryID, docID, rank.toString, score, runID)
}

object TrecLikeResult {
  val header = "QueryID\tQ0\tDocID\tRank\tScore\tRunID"
}
