package yairs.retrieval

import yairs.model._
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
class BooleanRetriever(config: Configuration) extends StructuredRetriever with Logging {
  val name: String = "Boolean"

  val invBaseName = config.get("yairs.inv.basename")
  val isRanked = config.getBoolean("yairs.ranked")

  /**
   * A concrete implementation of the method, will determine whether to
   * @param query
   * @param runId
   * @return
   */
  def getResults(query: Query, runId: String) = {
    evaluate(query, runId, config)
  }

  protected def getInvertedFile(leaf: QueryTreeNode, scorer: (Int, Int, Int, Int) => Double): InvertedList = {
    InvertedList(FileUtils.getInvertedFile(invBaseName: String, leaf.term, leaf.field, leaf.defaultField, isHw2 = true), isRanked)
  }

  protected def termScorer(collectionFrequency: Int, documentFreq: Int, termFrequency: Int, documentLength: Int): Double = termFrequency

  /**
   * Call rolling apply, provide conjunct as function
   * @param invertedLists Inverted lists to be merged by AND
   * @return Resulting InvertedList (with scores)
   */
  protected def and(invertedLists: List[InvertedList]): InvertedList = {
    rollingApply(invertedLists,conjunct)
  }

  /**
   * Call rolling apply, provide the disjunct as operator
   * @param invertedLists Inverted lists to be merged by OR
   * @return Resulting InvertedList (with scores)
   */
  protected def or(invertedLists:List[InvertedList]):InvertedList ={
    rollingApply(invertedLists,disjunct)
  }

  /**
   * Rolling multiple inverted list into 2 each time operator on a function
   * @param invertedLists The list of inverted lists.
   * @param func The function to be applied pairwisely
   * @return The resulting after rolling through all inverted lists
   */
  private def rollingApply(invertedLists:List[InvertedList], func: (InvertedList,InvertedList) => InvertedList):InvertedList = {
    var isFirst = true //basically avoid empty list to enter conjunction operation
    invertedLists.foldLeft(InvertedList.empty())((mergingList, currentList) => {
      if (isFirst) {
        isFirst = false
        currentList
      }
      else {
        func(mergingList, currentList)
      }
    })
  }


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
            intersectedPostings.append(Posting(docId1, disjunctMatchScore(p1.score, p2.score)))
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

    InvertedList(collectionFreq, list1.totalTermCount, documentFreq, intersectedPostings.toList,0)
  }

  protected def disjunctMatchScore(p1Score: Double, p2Score: Double): Double = math.max(p1Score, p2Score)

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
            collectionFreq += math.min(list1.collectionFrequency, list2.collectionFrequency)
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
    InvertedList(collectionFreq, list1.totalTermCount, documentFreq, intersectedPostings.toList,0)
  }

  protected def conjunctMatchScore(p1Score: Double, p2Score: Double): Double = math.min(p1Score, p2Score)

  /**
   * Boolean does not support WEIGHT
   * @param invertedLists Inverted lists to be merged using WEIGHT
   * @param weights weights of of each list
   * @return Resulting Inverted list(with score)
   */
  protected def weight(invertedLists: List[InvertedList], weights: List[Double]): InvertedList = {
    throw new UnsupportedOperationException("%s does not support WEIGHT operation!".format(name))
  }

  /**
   * Boolean does not support UW
   * This will result in a virtual term, thus scoring function need to be provided
   * @param invertedLists Inverted list to be merged using UW
   * @param k  Proximity
   * @param scorer How to score a term
   * @param config The configuration of the system
   * @return
   */
  protected def unorderedWindow(invertedLists: List[InvertedList], k: Int, scorer: (Int, Int, Int, Int) => Double, config: Configuration): InvertedList = {
    throw new UnsupportedOperationException("%s does not support UW operation!".format(name))
  }
}
