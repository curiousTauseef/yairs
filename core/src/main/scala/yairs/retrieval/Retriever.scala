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
  val name:String

  /**
   * Wrapper to get the results
   * @param query
   * @param runId
   * @return
   */
  def getResults(query:Query,runId:String) : List[Result]

  protected def evaluate(query: Query, runId:String, sortById:Boolean,isStructured:Boolean): List[Result] = {
    val root = query.queryRoot
    log.debug("Evaluating query:")
    query.dump()

    val evalResults = if (sortById) evaluateQuery(root,isStructured).sortBy(posting => posting.docId) else evaluateQuery(root,isStructured).sortBy(posting => posting.score).reverse

    evalResults.zipWithIndex.foldLeft(List[Result]()) {
      case (results, (posting, zeroBasedRank)) => {
        val score =  if (sortById) 1 else posting.score
        val result = new TrecLikeResult(query.queryId, posting.docId, zeroBasedRank + 1, score, runId)
        result :: results
      }
    }.reverse
  }

  protected def evaluateQuery(root: QueryTreeNode, isStructured:Boolean): List[Posting] = {
    if (isStructured){
      evaluateStructuredQuery(root).postings
    } else{
      evaluateBowQuery(root)
    }
  }

  protected def evaluateStructuredQuery(root: QueryTreeNode):InvertedList

  protected def evaluateBowQuery(root:QueryTreeNode) : List[Posting]

  protected def getInvertedFile(leaf:QueryTreeNode, scorer :(Int,Int,Int,Int)=>Double = termScorer) : InvertedList

  protected def termScorer(collectionFrequency:Int,documentFreq:Int,termFrequency:Int,documentLength:Int):Double

  }
