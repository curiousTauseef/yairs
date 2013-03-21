package yairs.retrieval

import yairs.model._
import org.eintr.loglady.Logging
import yairs.util.{Configuration}
import scala._
import collection.mutable
import yairs.io.{FileUtils, BooleanQueryReader}
import java.io.{PrintWriter, File}

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 3/16/13
 * Time: 6:43 PM
 */
class BM25Retriever(config: Configuration) extends MultimergeSturcturedRetriever with Logging {
  val name: String = "BM25"
  private val invBaseName = config.get("yairs.inv.basename")
  private val documentCount = config.getInt("yairs.document.count").toDouble
  private val averageDocumentSize = config.getInt("yairs.document.average.size").toDouble

//  /**
//   *
//   * @param query
//   * @param runId
//   * @return
//   */
//  def getResults(query: Query, runId: String) = {
//    evaluate(query,runId,config)
//  }

  /**
   * Inverted list getting method for BM25
   * @param node
   * @param scorer a term scorer for how to score the inverted list
   * @return A scored inverted list
   */
  def getInvertedFile(node: QueryTreeNode,scorer :(Int,Int,Int,Int)=>Double) = {
    log.debug("Getting inverted file for [%s]".format(node.term))
    InvertedList(FileUtils.getInvertedFile(invBaseName: String, node.term, node.field, node.defaultField, isHw2 = true),scorer,config)
  }

  /**
   * Term scorer for BM25
   * @param collectionFrequency
   * @param documentFreq
   * @param termFrequency
   * @param documentLength
   * @return The score given all the statistics
   */
  protected def termScorer(collectionFrequency:Int,documentFreq:Int,termFrequency:Int,documentLength:Int):Double = {
    val k_1 = config.getDouble("yairs.bm25.k1")
    val b = config.getDouble("yairs.bm25.b")
    //val k_3 = 0    //TODO: Currently no user specific query weights are supplied and implemented
    math.log10((documentCount - documentFreq+ 0.5) / (documentFreq + 0.5)) * termFrequency / (termFrequency + k_1 * ((1 - b) + b * documentLength / averageDocumentSize))
  }

  /**
   * BM25 does not support weight here
   * @param invertedLists Inverted lists to be merged using WEIGHT
   * @param weights weights of of each list
   * @return Resulting Inverted list(with score)
   */
  protected def weight(invertedLists: List[InvertedList], weights: List[Double]): InvertedList = {
     throw new UnsupportedOperationException("%s does not support WEIGHT operation!".format(name))
  }
}