package yairs.retrieval

import yairs.model._
import yairs.io.BooleanQueryReader
import java.io.File
import yairs.util.FileUtils
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
class BooleanRetriever(ranked:Boolean) extends Retriever with Logging {
  def evaluate(query: Query, runId: String): List[Result] = {
    val bQuery = query.asInstanceOf[BooleanQuery]
    val root = bQuery.queryRoot
    log.debug("Evaluating query:")
    bQuery.dump()

    evaluateNode(root).zipWithIndex.foldLeft(List[Result]()) {
      case (results, (posting, rank)) => {
        val result = new TrecLikeResult(bQuery.queryId, posting.docId, rank, posting.score, runId)
        result :: results
      }
    }.reverse
  }

  private def evaluateNode(node: QueryTreeNode): List[Posting] = {
    //log.debug("Evaluating node:")
    //node.dump()
    if (node.isLeaf) {
      InvertedList(FileUtils.getInvertedFile(node.term),ranked).postings
    }
    else {
      val childLists = node.children.foldLeft(List[List[Posting]]())((lists, child) => {
        if (child.isStop)
          lists
        else
          evaluateNode(child) :: lists
      })
      mergePostingLists(childLists, node)
    }
  }

  private def mergePostingLists(postingLists: List[List[Posting]], node: QueryTreeNode): List[Posting] = {
    var isFirst = true //basically avoid empty list to enter conjunction operation
    postingLists.foldLeft(List[Posting]())((mergingList, currentList) => {
      if (isFirst) {
        isFirst = false
        currentList
      }
      else intersectPostingLists(mergingList, currentList, node)
    })
  }

  private def intersectPostingLists(list1: List[Posting], list2: List[Posting], node: QueryTreeNode): List[Posting] = {
    if (node.isLeaf) throw new IllegalArgumentException("No intersection to do on leaf node")

    if (node.operator == QueryOperator.AND) {
      conjunct(list1, list2)
    } else if (node.operator == QueryOperator.OR) {
      disjunct(list1, list2)
    } else if (node.operator == QueryOperator.NEAR) {
      positionIntersect(list1, list2)
    } else {
      null
    }
  }

  private def positionIntersect(list1: List[Posting], list2: List[Posting]): List[Posting] = {
    null
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
          intersectedPostings.append(Posting(docId1))
          if (docId1 == docId2) {
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
            intersectedPostings.append(Posting(docId1))
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
    val start = System.nanoTime

    val qr = new BooleanQueryReader()
    val br = new BooleanRetriever()
    testQuerySet(qr, br)
    println("time: " + (System.nanoTime - start) / 1e9 + "s")
  }

  def testQuerySet(qr: BooleanQueryReader, br: BooleanRetriever) {
    val queries = qr.getQueries(new File("data/queries.txt"))
    queries.foreach(query => {
      val results = br.evaluate(query, "querySetTest")
      log.debug("Number of documents retrieved: " + results.length)
      if (results.length == 0) {
        log.debug("Really?")
        sys.exit()
      }
      //results.foreach(println)
    })
  }

  def testQuery(qr: BooleanQueryReader, br: BooleanRetriever, query: BooleanQuery) {
    val results = br.evaluate(qr.getQuery("1", "#OR arizona casino"), "singleQueryTest")
    log.debug("Number of documents retrieved: " + results.length)
    //results.foreach(println)
  }
}