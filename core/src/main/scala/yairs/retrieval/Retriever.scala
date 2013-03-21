package yairs.retrieval

import yairs.model._
import yairs.util.{Configuration}
import org.eintr.loglady.Logging

/**
 * Created with IntelliJ IDEA.
 * User: Hector
 * Date: 2/21/13
 * Time: 12:55 AM
 * To change this template use File | Settings | File Templates.
 */
trait Retriever extends Logging{
  //sub class need to clarified the name
  val name:String

  /**
   * Wrapper to get the results
   * @param query
   * @param runId
   * @return
   */
  def getResults(query:Query,runId:String, config:Configuration) : List[Result]  = {
    evaluate(query, runId, config)
  }

  protected def evaluate(query: Query, runId:String, config:Configuration): List[Result] = {
    val isRanked = config.getBoolean("yairs.ranked")

    val root = query.queryRoot
    log.debug("Evaluating query:")
    query.dump()

    val unSortedResults = evaluateQuery(root, config).postings.sortBy(posting => -posting.docId)

    val evalResults = if (!isRanked) unSortedResults else unSortedResults.sortBy(posting => -posting.score)

    //val evalResults = if (!isRanked) evaluateQuery(root, config).postings.sortBy(posting => posting.docId) else evaluateQuery(root, config).postings.sortBy(posting => posting.score).reverse

    evalResults.zipWithIndex.foldLeft(List[Result]()) {
      case (results, (posting, zeroBasedRank)) => {
        val score =  if (!isRanked) 1 else posting.score
        val result = new TrecLikeResult(query.queryId, posting.docId, zeroBasedRank + 1, score, runId)
        result :: results
      }
    }.reverse
  }

  /**
   * Core method, how to evaluate a query, sub classes have concrete implementation
   * @param root  Root node of the query
   * @param config System configuration
   * @return Final scored inverted list
   */
  protected def evaluateQuery(root: QueryTreeNode,config:Configuration): InvertedList

  /**
   * Get the inverted list (scored) of a term
   * @param leaf leaf node, which is associated with a term
   * @param scorer a term scorer for how to score the inverted list
   * @return A scored inverted list
   */
  protected def getInvertedFile(leaf:QueryTreeNode, scorer :(Int,Int,Int,Int)=>Double = termScorer) : InvertedList

  /**
   * A term scorer is needed for any retriever
   * @param collectionFrequency
   * @param documentFreq
   * @param termFrequency
   * @param documentLength
   * @return The score given all the statistics
   */
  protected def termScorer(collectionFrequency:Int,documentFreq:Int,termFrequency:Int,documentLength:Int):Double

  }
