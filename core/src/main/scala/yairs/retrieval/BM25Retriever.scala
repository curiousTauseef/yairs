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
class BM25Retriever(invFileBaseName: String, config: Configuration) extends Retriever with Logging {
  def getInvertedFile(node: QueryTreeNode) = InvertedList(FileUtils.getInvertedFile(invFileBaseName: String, node.term, node.field, node.field == node.defaultField,true),true,true)

  val documentCount = config.getInt("yairs.document.count").toDouble
  val averageDocumentSize = config.getInt("yairs.document.average.size").toDouble
  val vocabularySize = config.getInt("yairs.vocabulary.size").toDouble
  val totalWordCount = config.getInt("yairs.word.count").toDouble

  protected def evaluateNode(node: QueryTreeNode): List[Posting] = {
    //ugly! will refactor
    if (node.children!=null)
      bm25Default(node.children)
    else
      bm25Default(List(node))
  }

  def bm25Default(nodes: List[QueryTreeNode]): List[Posting] = {
    val documentScores = new mutable.HashMap[Int, Double]()

    nodes.foreach(node => {
      val invertedList = getInvertedFile(node)
      val df = invertedList.documentFrequency
      invertedList.postings.foreach(posting => {
        val docId = posting.docId
        val score = bm25Weight(df, posting.tf, posting.length)
        if (documentScores.contains(docId)) {
          documentScores(docId) += score
        } else {
          documentScores(docId) = score
        }
      }
      )
    })

    documentScores.toList.map{
      case (docId,score) => Posting(docId,score)
    }.toList
  }

  def bm25Weight(df: Int, tf: Int, docLen: Int): Double = {
    val k_1 = 1.2
    val b = 0.75
    val k_3 = 0
    math.log10((documentCount - df + 0.5) / (df + 0.5)) * tf / (tf + k_1 * ((1 - b) + b * docLen / averageDocumentSize))
  }

}

object BM25Retriever extends Logging {
  def main(args: Array[String]) {
    if (args.length == 0) {
      log.error("Please supply the configuration file path as command line parameter")
      System.exit(1)
    }
    val configurationFileName = args(0)
    val config = new Configuration(configurationFileName)

    val queryFileName = config.get("yairs.query.path")
    val outputDir = config.get("yairs.output.path")
    val invBaseName = config.get("yairs.inv.basename")
    val runId = config.get("yairs.run.id")
    val numResults = config.getInt("yairs.run.results.num")

    val start = System.nanoTime
    val qr = new BooleanQueryReader(config)
    val br = new BM25Retriever(invBaseName,config)
    testQuerySet(queryFileName, outputDir, qr, br, runId,numResults, false)

  }

  /**
   * Method to run a query set
   * @param queryFilePath     The path to the query file
   * @param outputDirectory   The output direcotry to store the results
   * @param qr  QueryReader object
   * @param br  Boolean Retriever object
   * @param runId A String used as a run ID
   * @param numResultsToOutput  Number of results to output
   */
  def testQuerySet(queryFilePath: String, outputDirectory: String, qr: BooleanQueryReader, br: Retriever, runId: String,  numResultsToOutput:Int, rankById:Boolean) {
    val queries = qr.getQueries(new File(queryFilePath))
    val writer = new PrintWriter(new File(outputDirectory + "/%s".format(runId)))
    writer.write(TrecLikeResult.header + "\n")

    queries.foreach(query => {
      val results = br.evaluate(query, runId,rankById)
      val resultsToOutput = if (numResultsToOutput > 0) results.take(numResultsToOutput) else results
      log.debug("Number of documents retrieved: " + results.length)
      if (results.length == 0) {
        log.error("Really? 0 document retrieved?")
      }
      println("==================Top 5 results=================")
      println(TrecLikeResult.header)
      results.take(5).foreach(println)
      println("================================================")
      resultsToOutput.foreach(r => writer.write(r.toString + "\n"))
    })
    writer.close()
  }
}
