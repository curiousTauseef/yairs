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
class BooleanRetriever(val invFileBaseName:String, ranked: Boolean = true) extends Retriever with Logging {
  def evaluate(query: Query, runId: String): List[Result] = {
    val bQuery = query.asInstanceOf[BooleanQuery]
    val root = bQuery.queryRoot
    log.debug("Evaluating query:")
    bQuery.dump()

    evaluateNode(root).sortBy(posting => posting.score).reverse.zipWithIndex.foldLeft(List[Result]()) {
      case (results, (posting, zeroBasedRank)) => {
        val result = new TrecLikeResult(bQuery.queryId, posting.docId, zeroBasedRank + 1, posting.score, runId)
        result :: results
      }
    }.reverse
  }

  private def evaluateNode(node: QueryTreeNode): List[Posting] = {
    if (node.isLeaf) {
      InvertedList(FileUtils.getInvertedFile(invFileBaseName:String, node.term, node.field), ranked).postings
    }
    else {
      //      node.children.foreach(node => node.dump())
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
    if (iter1.hasNext && iter2.hasNext) {
      var p1 = iter1.next()
      var p2 = iter2.next()

      breakable {
        while (true) {
          val docId1 = p1.docId
          val docId2 = p2.docId
          if (docId1 == docId2) {
            intersectedPostings.append(Posting(docId1, math.max(p1.score, p2.score)))
            if (!(iter1.hasNext && iter2.hasNext)) break
            p1 = iter1.next()
            p2 = iter2.next()
          } else if (docId1 < docId2) {
            intersectedPostings.append(Posting(docId1, p1.score))
            if (iter1.hasNext) p1 = iter1.next()
            else {
              intersectedPostings.appendAll(iter2)
              break
            }
          } else {
            intersectedPostings.append(Posting(docId2, p2.score))
            if (iter2.hasNext) p2 = iter2.next()
            else {
              intersectedPostings.appendAll(iter1)
              break
            }
          }
        }
      }
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
    val configurationFileName = args(0)
    val config = new Configuration(configurationFileName)

    val queryFileName = config.get("yairs.query.path")
    val outputDir = config.get("yairs.output.path")
    val stopWordFilePath = config.get("yairs.stoplist.path")
    val invBaseName = config.get("yairs.inv.basename")
    val runId = config.get("yairs.run.id")

    val start = System.nanoTime
    val qr = new BooleanQueryReader("#OR",new File(stopWordFilePath))
    val br = new BooleanRetriever(invBaseName,true)
    testQuerySet(queryFileName,outputDir,qr, br, runId,15)
//    testQuery("data/sample-output",qr, br,"97","#NEAR/1 (south africa)")
//    testQuery("sample-output",qr, br,"101","#OR (obama #NEAR/2 (family tree))")
//    testQuery("sample-output",qr, br,"102","#OR (espn sports)")
    println("time: " + (System.nanoTime - start) / 1e9 + "s")
  }

  /**
   * Method to run a query set
   * @param queryFilePath     The path to the query file
   * @param outputDirectory   The output direcotry to store the results
   * @param qr  QueryReader object
   * @param br  Boolean Retriever object
   * @param runId A String used as a run ID
   * @param k  Number of queries to run
   */
  def testQuerySet(queryFilePath:String, outputDirectory:String,qr: BooleanQueryReader, br: BooleanRetriever, runId: String, k:Int) {
//    val queries = qr.getQueries(new File("data/queries.txt"))
//    val writer = new PrintWriter(new File("data/sample-output/%s".format(runId)))
    val queries = qr.getQueries(new File(queryFilePath))
    val writer = new PrintWriter(new File(outputDirectory+"/%s".format(runId)))
    writer.write(TrecLikeResult.header + "\n")

    val queriesToProcess = if (k >0) queries.take(k) else queries

    queriesToProcess.foreach(query => {
      val results = br.evaluate(query, runId)
      log.debug("Number of documents retrieved: " + results.length)
      if (results.length == 0) {
        log.error("Really? 0 document retrieved?")
        //sys.exit()
      }
      //      println("==================Top 5 results=================")
      //      println(TrecLikeResult.header)
      //      results.take(5).foreach(println)
      //      println("================================================")
      results.foreach(r => writer.write(r.toString + "\n"))
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
   */
  def testQuery(outputDirectory:String, qr: BooleanQueryReader, br: BooleanRetriever, queryId:String,queryString:String) {
    //val results = br.evaluate(qr.getQuery("1", "#OR obama family"), "singleQueryTest")
    //val results = br.evaluate(qr.getQuery("1", "#OR arizona states"), "singleQueryTest")
//    val results = br.evaluate(qr.getQuery("1", "#AND (#AND (arizona states) obama)"), "singleQueryTest")
    //val results = br.evaluate(qr.getQuery("1", "#AND (#NEAR/1 (arizona states) obama)"), "singleQueryTest")
    //val results = br.evaluate(qr.getQuery("1", "#NEAR/1 (arizona states)"), "singleQueryTest")


    //val results = br.evaluate(qr.getQuery(queryId,"#NEAR/2 (family tree)"),runId)
    //val results = br.evaluate(qr.getQuery(queryId,"#OR (obama #NEAR/2 (family tree))"),runId)
    //val results = br.evaluate(qr.getQuery(queryId,"#OR (espn sports)"),"run"+queryId)
    val results = br.evaluate(qr.getQuery(queryId,queryString),"run"+queryId)
    val writer = new PrintWriter(new File(outputDirectory+"/%s.txt".format("run"+queryId)))
    writer.write(TrecLikeResult.header+"\n")

    results.foreach(r => writer.write(r.toString + "\n"))

    writer.close()
    log.debug("Number of documents retrieved: " + results.length)
    println("=================Top 10 results=================")
    results.take(10).foreach(println)
    println("================================================")
  }
}