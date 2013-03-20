package yairs.eval

import org.eintr.loglady.Logging
import yairs.util.Configuration
import yairs.io.{QueryReader, BooleanQueryReader}
import yairs.retrieval.{IndriRetriever, BM25Retriever, Retriever, BooleanRetriever}
import java.io.{File, PrintWriter}
import yairs.model.TrecLikeResult

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 3/17/13
 * Time: 4:45 PM
 */

/**
 * Just a simple runner that call different functions based on the property file
 */
object RetrieverRunner extends Logging {
  def main(args: Array[String]) {
    if (args.length == 0) {
      log.error("Please supply the configuration file path as command line parameter")
      System.exit(1)
    }
    val configurationFileName = args(0)
    val config = new Configuration(configurationFileName)

    val retrieverName = config.get("yairs.retriever.name")

    val start = System.nanoTime

    val retriever =
    if (retrieverName == "boolean")
      new BooleanRetriever(config)
    else if (retrieverName == "bm25")
      new BM25Retriever(config)
    else if (retrieverName == "indri")
      new IndriRetriever(config)
    else
      new IndriRetriever(config)

    val queryReader = new BooleanQueryReader(config)

    val queryFileName = config.get("yairs.query.path")
    val outputDir = config.get("yairs.output.path")
    val runId = config.get("yairs.run.id")
    val numResults = config.getInt("yairs.run.results.num")

    testQuerySet(queryFileName, outputDir, queryReader,retriever, runId, numResults)

    log.info("Execution time for [%s] : [%s] s".format(retrieverName,(System.nanoTime - start) / 1e9))
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
  def testQuerySet(queryFilePath: String, outputDirectory: String, qr: QueryReader, br: Retriever, runId: String, numResultsToOutput: Int) {
    val queries = qr.getQueries(new File(queryFilePath))
    val writer = new PrintWriter(new File(outputDirectory + "/%s".format(runId)))
    writer.write(TrecLikeResult.header + "\n")

    queries.foreach(query => {
      val results = br.getResults(query, runId)
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

  /**
   * Test individual query
   *
   * @param outputDirectory directory to output the result
   * @param qr A Query Reader object
   * @param br A boolean retriever object
   * @param queryId A string indicating the queryID
   * @param queryString Query to be run
   * @param k top k results returned
   */
  def testQuery(outputDirectory: String, qr: QueryReader, br: Retriever, queryId: String, queryString: String, k: Int) {
    val results = br.getResults(qr.getQuery(queryId, queryString), "run" + queryId)
    val writer = new PrintWriter(new File(outputDirectory + "/%s.txt".format("run" + queryId)))
    writer.write(TrecLikeResult.header + "\n")

    val resultsToOutput = if (k > 0) results.take(k) else results
    resultsToOutput.foreach(r => writer.write(r.toString + "\n"))

    writer.close()
    log.debug("Number of documents retrieved: " + results.length)
    println("=================Top 10 results=================")
    results.take(10).foreach(println)
    println("================================================")
  }


  @deprecated
  def runBoolean(config: Configuration) {
    //***edit the configuration file to change the following value
    val queryFileName = config.get("yairs.query.path")
    val outputDir = config.get("yairs.output.path")
    val runId = config.get("yairs.run.id")
    val numResults = config.getInt("yairs.run.results.num")

    val qr = new BooleanQueryReader(config)
    val br = new BooleanRetriever(config)
    testQuerySet(queryFileName, outputDir, qr, br, runId, numResults)

    //***uncomment the following queries to see individual queries
    //***they are also used to generate the sample queries
    //        testQuery("data/sample-output",qr, br,"97","#NEAR/1 (south africa)",100)
    //        testQuery("data/sample-output",qr, br,"100","#NEAR/2 (family tree)",100)
    //        testQuery("data/sample-output",qr, br,"101","#OR (obama #NEAR/2 (family tree))",100)
    //        testQuery("data/sample-output",qr, br,"102","#OR (espn sports)",100)
  }

  @deprecated
  def runBm25(config:Configuration) {
    val queryFileName = config.get("yairs.query.path")
    val outputDir = config.get("yairs.output.path")
    val runId = config.get("yairs.run.id")
    val numResults = config.getInt("yairs.run.results.num")

    val qr = new BooleanQueryReader(config)
    val br = new BM25Retriever(config)
    testQuerySet(queryFileName, outputDir, qr, br, runId, numResults)

  }
}
