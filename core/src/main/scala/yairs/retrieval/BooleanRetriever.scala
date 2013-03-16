package yairs.retrieval

import yairs.model._
import yairs.io.BooleanQueryReader
import java.io.{PrintWriter, File}
import yairs.util.{Configuration, FileUtils}
import org.eintr.loglady.Logging
import scala.util.control.Breaks._
import collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: Hector
 * Date: 2/21/13
 * Time: 12:56 AM
 * To change this template use File | Settings | File Templates.
 */
class BooleanRetriever(val invFileBaseName: String, ranked: Boolean = true) extends Retriever with Logging {
  def evaluate(query: Query, runId: String): List[Result] = {
    val bQuery = query.asInstanceOf[BooleanQuery] //a little bit ugly, huh?
    val root = bQuery.queryRoot
    log.debug("Evaluating query:")
    bQuery.dump()

    val evalResults = if (ranked) evaluateNode(root).sortBy(posting => posting.score) else evaluateNode(root).sortBy(posting => posting.docId)

    evalResults.zipWithIndex.foldLeft(List[Result]()) {
      case (results, (posting, zeroBasedRank)) => {
        val result = new TrecLikeResult(bQuery.queryId, posting.docId, zeroBasedRank + 1, posting.score, runId)
        result :: results
      }
    }
  }

  private def evaluateNode(node: QueryTreeNode): List[Posting] = {
    if (node.isLeaf) {
      InvertedList(FileUtils.getInvertedFile(invFileBaseName: String, node.term, node.field), ranked).postings
    }
    else {
      val childLists = node.children.foldLeft(List[List[Posting]]())((lists, child) => {
        if (child.isStop)
          lists
        else
          evaluateNode(child) :: lists
      }).reverse //ensure evaluation sequences
      val optimizedChildLists = if (node.operator == QueryOperator.AND) childLists.sortBy(l => l.length) else childLists
      mergePostingLists(optimizedChildLists, node)
    }
  }

  private def mergePostingLists(postingLists: List[List[Posting]], node: QueryTreeNode): List[Posting] = {
    var isFirst = true //basically avoid empty list to enter conjunction operation
    postingLists.foldLeft(List[Posting]())((mergingList, currentList) => {
      if (isFirst) {
        isFirst = false
        currentList
      }
      else intersect2PostingLists(mergingList, currentList, node)
    })
  }

  private def intersect2PostingLists(list1: List[Posting], list2: List[Posting], node: QueryTreeNode): List[Posting] = {
    if (node.isLeaf) throw new IllegalArgumentException("No intersection to do on leaf node")

    if (node.operator == QueryOperator.AND) {
      conjunct(list1, list2)
    } else if (node.operator == QueryOperator.OR) {
      disjunct(list1, list2)
    } else if (node.operator == QueryOperator.NEAR) {
      positionIntersect(list1, list2, node.proximity)
    } else {
      null
    }
  }

  private def positionIntersect(list1: List[Posting], list2: List[Posting], k: Int): List[Posting] = {
    val iter1 = list1.iterator
    val iter2 = list2.iterator

    val intersectedPostings = new ListBuffer[Posting]()
    if (iter1.hasNext && iter2.hasNext) {
      var p1 = iter1.next()
      var p2 = iter2.next()

      breakable {
        while (true) {
          val docId1 = p1.docId
          val docId2 = p2.docId

          if (docId1 == docId2) {
            val nearMatchesList = getNearMatchedPositions(p1.positions, p2.positions, k)
            val matches = nearMatchesList.length
            if (matches > 0) {
              val score = if (ranked) matches else 1
              intersectedPostings.append(Posting(docId1, nearMatchesList.map(_._1), score))
            }
            if (!(iter1.hasNext && iter2.hasNext)) {
              break
            }
            p1 = iter1.next()
            p2 = iter2.next()
          } else if (docId1 < docId2) {
            if (!iter1.hasNext) break
            p1 = iter1.next()
          } else {
            if (!iter2.hasNext) break
            p2 = iter2.next()
          }
        }
      }
    }
    intersectedPostings.toList
  }

  /**
   * Intersect two positions list, return the positions of the second list
   * @param positions1
   * @param positions2
   * @return
   */
  private def getNearMatchedPositions(positions1: List[Int], positions2: List[Int], k: Int): List[(Int, Int)] = {
    val iter1 = positions1.iterator
    val iter2 = positions2.iterator

    val results = new ListBuffer[(Int, Int)]()

    if (iter1.hasNext && iter2.hasNext) {
      var pp1 = iter1.next()
      var pp2 = iter2.next()

      breakable {
        while (true) {
          if (pp2 >= pp1) {
            if (pp2 - pp1 <= k) {
              results.append((pp1, pp2))
            }
            if (!iter1.hasNext) break
            pp1 = iter1.next()
          } else {
            if (!iter2.hasNext) break
            pp2 = iter2.next()
          }
        }
      }
    }
    results.toList
  }

  /**
   * OR operation for 2 posting lists intersection
   * @param list1
   * @param list2
   * @return
   */
  private def disjunct(list1: List[Posting], list2: List[Posting]): List[Posting] = {
    val iter1 = list1.iterator
    val iter2 = list2.iterator

    val intersectedPostings = new ListBuffer[Posting]()

    log.debug("Disjunct!")


    if (iter1.hasNext && iter2.hasNext) {
      var p1 = iter1.next()
      var p2 = iter2.next()

      breakable{
        while (true) {
          val docId1 = p1.docId
          val docId2 = p2.docId

          if (p1.docId == 890586) println("p1 "+p2.docId)
          if (p2.docId == 890586) println("p2 "+p1.docId)

          if (p1.docId == 890585) println("p1!")
          if (p2.docId == 890585) println("p2!")

          if (docId1 == docId2) {
            intersectedPostings.append(Posting(docId1, math.max(p1.score, p2.score)))
            if (!iter1.hasNext) {
              break
            } else if (!iter2.hasNext){
              break
            }
            p1 = iter1.next()
            p2 = iter2.next()
          } else if (docId1 < docId2) {
            intersectedPostings.append(Posting(docId1,p1.score))
            if (!iter1.hasNext){
              intersectedPostings.append(Posting(docId2,p2.score))
              break
            }
            p1 = iter1.next()
          } else {
            intersectedPostings.append(Posting(docId2,p2.score))
            if (!iter2.hasNext){
              intersectedPostings.append(Posting(docId1,p1.score))
              break
            }
            p2 = iter2.next()
          }
        }
      }
    }

    while(iter1.hasNext){
      val p = iter1.next()
      intersectedPostings.append(Posting(p.docId,p.score))
    }

    while(iter2.hasNext){
      val p = iter2.next()
      intersectedPostings.append(Posting(p.docId,p.score))
    }

    intersectedPostings.toList
  }

  /**
   * AND operation for 2 postings lists intersection
   * @param list1
   * @param list2
   * @return  Merged posting list
   */
  private def conjunct(list1: List[Posting], list2: List[Posting]): List[Posting] = {
    val iter1 = list1.iterator
    val iter2 = list2.iterator

    val intersectedPostings = new ListBuffer[Posting]()
    if (iter1.hasNext && iter2.hasNext) {
      var p1 = iter1.next()
      var p2 = iter2.next()

      breakable {
        while (true) {
          val docId1 = p1.docId
          val docId2 = p2.docId

          if (docId1 == docId2) {
            intersectedPostings.append(Posting(docId1, math.min(p1.score, p2.score)))
            if (!(iter1.hasNext && iter2.hasNext)) {
              break
            }
            p1 = iter1.next()
            p2 = iter2.next()
          } else if (docId1 < docId2) {
            if (!iter1.hasNext) break
            p1 = iter1.next()
          } else {
            if (!iter2.hasNext) break
            p2 = iter2.next()
          }
        }
      }
    }
    intersectedPostings.toList
  }

}

object BooleanRetriever extends Logging {
  def main(args: Array[String]) {
    if (args.length == 0) {
      log.error("Please supply the configuration file path as command line parameter")
      System.exit(1)
    }
    val configurationFileName = args(0)
    val config = new Configuration(configurationFileName)

    runBoolean(config)
  }

  def runBoolean(config:Configuration){
    //***edit the configuration file to change the following value
    val queryFileName = config.get("yairs.query.path")
    val outputDir = config.get("yairs.output.path")
    val stopWordFilePath = config.get("yairs.stoplist.path")
    val invBaseName = config.get("yairs.inv.basename")
    val runId = config.get("yairs.run.id")
    val isRankStr = config.get("yairs.boolean.ranked")
    val isRanked = if (isRankStr == "true") true else false
    val defaultOperator = config.getDefaultOperator("yaris.boolean.operator.default")
    val numResults = config.getInt("yairs.run.results.num")

    val start = System.nanoTime
    val qr = new BooleanQueryReader(defaultOperator, new File(stopWordFilePath))
    val br = new BooleanRetriever(invBaseName, isRanked)
    testQuerySet(queryFileName, outputDir, qr, br, runId,numResults)

    //***uncomment the following queries to see individual queries
    //***they are also used to generate the sample queries
    //        testQuery("data/sample-output",qr, br,"97","#NEAR/1 (south africa)",100)
    //        testQuery("data/sample-output",qr, br,"100","#NEAR/2 (family tree)",100)
    //        testQuery("data/sample-output",qr, br,"101","#OR (obama #NEAR/2 (family tree))",100)
    //        testQuery("data/sample-output",qr, br,"102","#OR (espn sports)",100)
    println("time: " + (System.nanoTime - start) / 1e9 + "s")
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
  def testQuerySet(queryFilePath: String, outputDirectory: String, qr: BooleanQueryReader, br: BooleanRetriever, runId: String,  numResultsToOutput:Int) {
    val queries = qr.getQueries(new File(queryFilePath))
    val writer = new PrintWriter(new File(outputDirectory + "/%s".format(runId)))
    writer.write(TrecLikeResult.header + "\n")

    queries.foreach(query => {
      val results = br.evaluate(query, runId)
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
  def testQuery(outputDirectory: String, qr: BooleanQueryReader, br: BooleanRetriever, queryId: String, queryString: String, k: Int) {
    val results = br.evaluate(qr.getQuery(queryId, queryString), "run" + queryId)
    val writer = new PrintWriter(new File(outputDirectory + "/%s.txt".format("run" + queryId)))
    writer.write(TrecLikeResult.header + "\n")

    val resultsToOutput = if (k>0) results.take(k) else results
    resultsToOutput.foreach(r=> writer.write(r.toString + "\n"))

    writer.close()
    log.debug("Number of documents retrieved: " + results.length)
    println("=================Top 10 results=================")
    results.take(10).foreach(println)
    println("================================================")
  }
}
