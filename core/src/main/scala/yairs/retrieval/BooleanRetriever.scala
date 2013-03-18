package yairs.retrieval

import yairs.model._
import yairs.io.{QueryReader, BooleanQueryReader}
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
class BooleanRetriever(config:Configuration) extends StructuredRetriever with Logging {
  val name: String = "Boolean"

  val isRanked = config.getBoolean("yairs.ranked")
  val invBaseName = config.get("yairs.inv.basename")

  def getResults(query: Query, runId: String) = {
    evaluate(query,runId,sortById = !isRanked,isStructured = true)
  }

  protected def evaluateBowQuery(root: QueryTreeNode): List[Posting] = {
    throw new UnsupportedOperationException("Boolean retriever is not implemented for BOW retrieval")
  }

  protected def getInvertedFile(leaf: QueryTreeNode, scorer: (Int, Int, Int, Int) => Double): InvertedList = {
    InvertedList(FileUtils.getInvertedFile(invBaseName: String, leaf.term, leaf.field, leaf.defaultField, isHw2 = true), isRanked)
  }

  protected def termScorer(collectionFrequency: Int, documentFreq: Int, termFrequency: Int, documentLength: Int): Double = termFrequency

  /**
   * Interesect two list based on postions and retain sequential. In other words, the "NEAR" operator
   * @param list1 The list to be intersect
   * @param list2 The other list to be intersect
   * @param k The proximity distance allowed
   * @return  Resulting posting list
   */
  protected def twoWayMerge(list1: InvertedList, list2: InvertedList, k: Int): InvertedList = {
    val iter1 = list1.postings.iterator
    val iter2 = list2.postings.iterator

    //totalTermCount is a duplicated statistic in each inverted list!
    var documentFreq = 0
    var collectionFreq = 0

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
              collectionFreq += matches
              documentFreq += 1
              val score = matches
              intersectedPostings.append(Posting(docId1, nearMatchesList.map(_._2), score))
            }
            if (!(iter1.hasNext && iter2.hasNext)) {
              break()
            }
            p1 = iter1.next()
            p2 = iter2.next()
          } else if (docId1 < docId2) {
            if (!iter1.hasNext) break()
            p1 = iter1.next()
          } else {
            if (!iter2.hasNext) break()
            p2 = iter2.next()
          }
        }
      }
    }
    InvertedList(collectionFreq, list1.totalTermCount, documentFreq,intersectedPostings.toList)
  }

  /**
   * Intersect two positions list, return the positions pair where they match the proximity requirement
   * @param positions1  Position list for the first document
   * @param positions2  Position list for the second document
   * @return A list of tuples where they match, normally the length of this list is what you concern
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
            //This is the implementation that forward all the points, as discussed in email discussion
            if (!(iter1.hasNext && iter2.hasNext)) {
              break()
            }
            pp1 = iter1.next()
            pp2 = iter2.next()
            //This is the implementation that forward the smaller points, which conform with the sample result given in HW1, but not correct according to email discussions
            //            if (!iter1.hasNext) {
            //              break()
            //            }
            //            pp1 = iter1.next()

          } else {
            if (!iter2.hasNext) break()
            pp2 = iter2.next()
          }
        }
      }
    }
    results.toList
  }

  protected def disjunctMatchScore(p1Score: Double, p2Score: Double): Double = math.max(p1Score,p2Score)

  /**
   * OR operation for 2 posting lists intersection
   * @param list1
   * @param list2
   * @return
   */
  protected def disjunct(list1: InvertedList, list2: InvertedList): InvertedList = {
    val iter1 = list1.postings.iterator
    val iter2 = list2.postings.iterator

    val intersectedPostings = new ListBuffer[Posting]()

    //totalTermCount is a duplicated statistic in each inverted list!
    var documentFreq = 0
    var collectionFreq = 0

    if (iter1.hasNext && iter2.hasNext) {
      var p1 = iter1.next()
      var p2 = iter2.next()
      breakable {
        while (true) {
          val docId1 = p1.docId
          val docId2 = p2.docId

          if (docId1 == docId2) {
            documentFreq += 1
            //Is this right?
            collectionFreq += math.max(list1.collectionFrequency, list2.collectionFrequency)
            intersectedPostings.append(Posting(docId1, disjunctMatchScore(p1.score,p2.score)))
            if (!(iter1.hasNext && iter2.hasNext)) {
              break()
            }
            p1 = iter1.next()
            p2 = iter2.next()
          } else if (docId1 < docId2) {
            intersectedPostings.append(Posting(docId1, p1.score))
            if (!iter1.hasNext) {
              intersectedPostings.append(Posting(docId2, p2.score))
              break()
            }
            p1 = iter1.next()
          } else {
            intersectedPostings.append(Posting(docId2, p2.score))
            if (!iter2.hasNext) {
              intersectedPostings.append(Posting(docId1, p1.score))
              break()
            }
            p2 = iter2.next()
          }
        }
      }
    }

    while (iter1.hasNext) {
      val p = iter1.next()
      intersectedPostings.append(Posting(p.docId, p.score))
    }

    while (iter2.hasNext) {
      val p = iter2.next()
      intersectedPostings.append(Posting(p.docId, p.score))
    }

    InvertedList(collectionFreq, list1.totalTermCount, documentFreq,intersectedPostings.toList)
  }

  /**
   * AND operation for 2 postings lists intersection
   * @param list1
   * @param list2
   * @return  Merged posting list
   */
  protected def conjunct(list1: InvertedList, list2: InvertedList): InvertedList = {
    val iter1 = list1.postings.iterator
    val iter2 = list2.postings.iterator

    val intersectedPostings = new ListBuffer[Posting]()

    //totalTermCount is a duplicated statistic in each inverted list!
    var documentFreq = 0
    var collectionFreq = 0
    if (iter1.hasNext && iter2.hasNext) {
      var p1 = iter1.next()
      var p2 = iter2.next()

      breakable {
        while (true) {
          val docId1 = p1.docId
          val docId2 = p2.docId

          if (docId1 == docId2) {
            intersectedPostings.append(Posting(docId1, conjunctMatchScore(p1.score, p2.score)))
            documentFreq += 1
            collectionFreq += math.min(list1.collectionFrequency,list2.collectionFrequency)
            if (!(iter1.hasNext && iter2.hasNext)) {
              break()
            }
            p1 = iter1.next()
            p2 = iter2.next()
          } else if (docId1 < docId2) {
            if (!iter1.hasNext) break()
            p1 = iter1.next()
          } else {
            if (!iter2.hasNext) break()
            p2 = iter2.next()
          }
        }
      }
    }
    InvertedList(collectionFreq, list1.totalTermCount, documentFreq,intersectedPostings.toList)
  }

  protected def conjunctMatchScore(p1Score: Double, p2Score: Double): Double = math.min(p1Score,p2Score)

  protected def unorderedWindow(): InvertedList = {
    throw new UnsupportedOperationException("Boolean retriever has not implemented unordered window.")
  }
}
