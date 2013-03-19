package yairs.retrieval

import yairs.model._
import org.eintr.loglady.Logging
import yairs.util.{FileUtils, Configuration}
import scala._
import collection.mutable
import yairs.io.BooleanQueryReader
import java.io.{PrintWriter, File}

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 3/16/13
 * Time: 6:43 PM
 */
class BM25Retriever(config: Configuration) extends BOWRetriever with Logging {
  val name: String = "BM25"
  val isStructured = config.getBoolean("yairs.query.isStructure")

  val invBaseName = config.get("yairs.inv.basename")
  val documentCount = config.getInt("yairs.document.count").toDouble
  val averageDocumentSize = config.getInt("yairs.document.average.size").toDouble
  val vocabularySize = config.getInt("yairs.vocabulary.size").toDouble
  val totalWordCount = config.getInt("yairs.word.count").toDouble

  def getResults(query: Query, runId: String) = {
    evaluate(query,runId,sortById = false,isStructured)
  }

  def getInvertedFile(node: QueryTreeNode,scorer :(Int,Int,Int,Int)=>Double) = {
    log.debug("Getting inverted file for [%s]".format(node.term))
    InvertedList(FileUtils.getInvertedFile(invBaseName: String, node.term, node.field, node.defaultField, isHw2 = true),scorer,config)
  }

  protected def termScorer(collectionFrequency:Int,documentFreq:Int,termFrequency:Int,documentLength:Int):Double = {
    val k_1 = 1.2
    val b = 0.75
    val k_3 = 0    //TODO: Currently no user specific query weights are supplied and implemented
    math.log10((documentCount - documentFreq+ 0.5) / (documentFreq + 0.5)) * termFrequency / (termFrequency + k_1 * ((1 - b) + b * documentLength / averageDocumentSize))
  }

  protected  def evaluateStructuredQuery(root: QueryTreeNode):InvertedList ={
    throw new UnsupportedOperationException("BM25 is currently not implemented for any structured queries")
  }
}