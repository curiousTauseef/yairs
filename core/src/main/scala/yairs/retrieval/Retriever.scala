package yairs.retrieval

import yairs.model._
import yairs.util.FileUtils
import org.eintr.loglady.Logging

/**
 * Created with IntelliJ IDEA.
 * User: Hector
 * Date: 2/21/13
 * Time: 12:55 AM
 * To change this template use File | Settings | File Templates.
 */
trait Retriever extends Logging{
  def evaluate(query: Query, runId:String, sortById:Boolean): List[Result] = {
    val root = query.queryRoot
    log.debug("Evaluating query:")
    query.dump()

    val evalResults = if (sortById) evaluateNode(root).sortBy(posting => posting.docId) else evaluateNode(root).sortBy(posting => posting.score)

    evalResults.zipWithIndex.foldLeft(List[Result]()) {
      case (results, (posting, zeroBasedRank)) => {
        val result = new TrecLikeResult(query.queryId, posting.docId, zeroBasedRank + 1, posting.score, runId)
        result :: results
      }
    }
  }

  protected def evaluateNode(node: QueryTreeNode): List[Posting]

  protected def getInvertedFile(node:QueryTreeNode) : InvertedList
}
