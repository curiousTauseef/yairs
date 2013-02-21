package yairs.model

/**
 * Created with IntelliJ IDEA.
 * User: Hector
 * Date: 2/21/13
 * Time: 1:16 AM
 * To change this template use File | Settings | File Templates.
 */
class TrecLikeResult(val queryID: String, val docID: String, val rank: Int, val score: Float, val runID: String) extends Result {
  override def toString = String.format("%s\tQ0%s\t%s\t%s\t%s", queryID, docID, rank.toString, score.toString, runID)
}

object TrecLikeResult {
  val header = "QueryID\tQ0\tDocID\tRank\tScore\tRunID"
}
