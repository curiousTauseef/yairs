package yairs.retrieval

import yairs.model._
import yairs.util.{Configuration, FileUtils}
import collection.mutable.ListBuffer
import util.control.Breaks._
import org.eintr.loglady.Logging

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 3/17/13
 * Time: 4:28 PM
 */
class IndriRetriever(config: Configuration) extends MultimergeSturcturedRetriever with Logging {
  val name: String = "Indri"

  private val invBaseName = config.get("yairs.inv.basename")
  private val documentCount = config.getInt("yairs.document.count").toDouble
  private val totalWordCount = config.getInt("yairs.word.count").toDouble

  /**
   * Wrapper to get the results
   * @param query
   * @param runId
   * @return
   */
  def getResults(query: Query, runId: String): List[Result] = {
    evaluate(query, runId,config)
  }

  protected def getInvertedFile(node: QueryTreeNode, scorer: (Int, Int, Int, Int) => Double = termScorer): InvertedList = {
    log.debug("Getting inverted file for [%s]".format(node.term))
    InvertedList(FileUtils.getInvertedFile(invBaseName: String, node.term, node.field, node.defaultField, isHw2 = true), scorer, config)
  }

  protected def termScorer(collectionFrequency: Int, documentFreq: Int, termFrequency: Int, documentLength: Int): Double = {
    val lambda = config.getDouble("yairs.indri.lamba")
    val mu = config.getDouble("yairs.indri.mu")
    val pirorMethod = config.get("yairs.indri.query.piror")

    val collectionPrior=
    if (pirorMethod == "wordBased")
       collectionFrequency / totalWordCount
    else
      documentFreq/documentCount
    val weight = lambda * (termFrequency + mu * collectionPrior) / (documentLength + mu) + (1 - lambda) * collectionPrior

    math.log10(weight)
  }

  override protected def weight(invertedLists: List[InvertedList], weights: List[Double]): InvertedList = {
    multiwayMerge(invertedLists,weights)
  }
}