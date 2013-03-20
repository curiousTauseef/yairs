package yairs.retrieval

import yairs.model.{InvertedList, Posting, QueryTreeNode}
import collection.mutable
import yairs.util.Configuration
import collection.mutable.ListBuffer
import yairs.retrieval.StructuredRetriever

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 3/17/13
 * Time: 8:16 PM
 */

/**
 * A trait that extends StructuredRetriever.
 * It provides methods using multi-merge algorithm
 * Such as UW and AND
 *
 * Known subclasses are IndriRetriever and BM25Retriever
 */
trait MultimergeSturcturedRetriever extends StructuredRetriever {
  protected def and(invertedLists: List[InvertedList]): InvertedList = {
    multiwayMerge(invertedLists,List.fill(invertedLists.length)(1))
  }

  /**
   * AND operation for multiple postings lists intersection
   * @param invertedLists Inverted lists to be merged
   * @param weights weights on each list, given uniform if non-specificed
   * @return Resulting inverted list (with score)
   */
  protected def multiwayMerge(invertedLists: List[InvertedList], weights: List[Double]): InvertedList = {
    val iters = invertedLists.map(list => list.postings.iterator).toArray
    val intersectedPostings = new ListBuffer[Posting]()

    val pointers = iters.map(iter => if (iter.hasNext) iter.next() else null)

    //the default score for AND is to apply and on all the query terms
    //in this case, we sum them up because it is logged
    //we also consider weight, for normal AND, weight will be 1
    val weightedDefaultScores = invertedLists.map(list => list.defaultScore).zip(weights).map {
      case (dScore, w) => dScore * w
    }
    val combinedDefaultScores = weightedDefaultScores.foldLeft(0.0)((comb, ws) => comb + ws)

    while (somePointerNotNull(pointers)) {
      val docIds = pointers.filterNot(_ == null).map(pointer => pointer.docId)
      val minDocId = docIds.reduceLeft((l, r) => if (r < l) r else l)
      var score = 0.0

      var index = 0
      pointers.foreach(pointer => {
        val weight = weights(index)

        if (pointer != null && pointer.docId == minDocId) {
          //scores are logged, so p^w becomes wlog(p)
          score += pointer.score * weight
          if (iters(index).hasNext) {
            pointers(index) = iters(index).next()
          } else {
            pointers(index) = null
          }

        } else {
          //scores are logged, so p^w becomes wlog(p)
          score += weightedDefaultScores(index)
        }

        index += 1
      })
      intersectedPostings.append(Posting(minDocId, score))
    }

    val documentFreq = intersectedPostings.foldLeft(0)((sum, posting) => {
      sum + posting.docLength
    })
    val totalCollectionFreq = invertedLists.foldLeft(0)((sum, list) => {
      sum + list.collectionFrequency
    })

    //We use the total document frequency amongst the inverted list as the document frequency
    //We use the total collection frequency amongst the inverted lists as the collection frequency
    InvertedList(totalCollectionFreq, invertedLists(0).totalTermCount, documentFreq, intersectedPostings.toList, combinedDefaultScores)
  }

  /**
   * Check if any pointer is null
   * @param pointers
   * @return true if one pointer is null
   */
  private def somePointerNotNull(pointers: Array[Posting]) = {
    pointers.foldLeft(false)((isNull, posting) => {
      isNull || posting != null
    })
  }

  /**
   * An multimerge implementation of Unordered window
   * @param invertedLists Inverted list to be merged using UW
   * @param k  Proximity
   * @param scorer How to score a term
   * @param config The configuration of the system
   * @return Resulting Inverted List for a virtual term (with score)
   */
   protected def unorderedWindow(invertedLists:List[InvertedList], k:Int, scorer:(Int,Int,Int,Int)=>Double, config:Configuration):InvertedList = {
    val iters = invertedLists.map(list => list.postings.iterator).toArray
    val intersectedPostings = new ListBuffer[Posting]()

    val pointers = iters.map(iter => if (iter.hasNext) iter.next() else null)

    //We sum up the term frequency of each UW amongst the inverted lists as the collection frequency
    var totalCollectionFreq = 0
    var totalDocumentFreq = 0
    while (allPointerNotNull(pointers)) {
      val docIds = pointers.map(pointer => pointer.docId)
      val minDocId = docIds.reduceLeft((l, r) => if (r < l) r else l)

      val isSameDoc = docIds.foldLeft(true)((isSame, r) => (r == minDocId) && isSame)

      val uwTermFreq = if (isSameDoc) {
        //log.debug("Is same doc:"+minDocId)
        val positionsList = pointers.map(pointer => pointer.positions)
        checkUWPositions(positionsList, k)
      } else 0 //0 means either no match, or these three terms does not has the same document

      //thus we got a match
      if (uwTermFreq > 0){
        totalCollectionFreq += uwTermFreq
        totalDocumentFreq += 1
        //log.debug("Get %s matches on UW in document %s".format(uwTermFreq,minDocId))

        // a dummy position list(empty) is added becuase it is not clear what position is for UW
        intersectedPostings.append(new Posting(minDocId, uwTermFreq,pointers(0).docLength,List[Int](),0))
      }


      //for pointers that equal to the min pointer, move next
      pointers.zipWithIndex.foreach {
        case (pointer, index) => {
          if (pointer.docId == minDocId) {
            if (iters(index).hasNext) {
              pointers(index) = iters(index).next()
            } else {
              pointers(index) = null
            }
          }
        }
      }
    }

    //we score them now becaue we now have the collection frequency and document frequency after going through the whole list
    //we always score postings while creating, so that in future operations, we don't need to recalculate in each loop
    val scoredPostings = intersectedPostings.map(posting=>{
      val score = termScorer(totalCollectionFreq,totalDocumentFreq,posting.tf,posting.docLength)
      new Posting(posting.docId,posting.tf,posting.docLength,posting.positions,score)
    }).toList
    InvertedList(totalCollectionFreq, invertedLists(0).totalTermCount, totalDocumentFreq, scoredPostings, termScorer,config)
  }

  /**
   * Check multiple position list for window constrain
   * @param positionsList
   * @param k Proximity
   * @return Number of matches
   */
  private def checkUWPositions(positionsList: Iterable[List[Int]], k: Int): Int = {
    val iters = positionsList.map(list => list.iterator).toArray
    val itersWithIndex = iters.zipWithIndex

    var numMatch = 0
    val mergedPostionsList = ListBuffer[Int]()

    val positions = iters.map(iter => if (iter.hasNext) iter.next() else -1).toArray
    while (allValidLocations(positions)) {
      if (matchUWCondition(positions, k)) {
        //we got a match
        numMatch += 1

        //advance all iterator
        itersWithIndex.foreach {
          case (iter, index) => {
            positions(index) = if (iter.hasNext) iter.next() else -1
          }
        }
      } else {
        //advnace smallest iterator
        val minPos = positions.reduceLeft((l, r) => if (r < l) r else l)
        positions.zipWithIndex.foreach {
          case (pos, index) => {
            if (pos == minPos) {
              positions(index) = if (iters(index).hasNext) iters(index).next() else -1
            }
          }
        }
      }
    }
    numMatch
  }

  /**
   * Check whether all location pointer are still valid
   * @param positions
   * @return
   */
  private def allValidLocations(positions: Iterable[Int]): Boolean = {
    positions.foldLeft(true)((isValid, position) => {
      isValid && !(position < 0)
    })
  }

  /**
   * Unorder window constrain for 3 words
   * @param locations
   * @param k
   * @return
   */
  private def matchUWCondition(locations: Iterable[Int], k: Int): Boolean = {
    val minLocation = locations.reduceLeft((l, r) => if (r < l) r else l)
    val maxLocation = locations.reduceLeft((l, r) => if (r > l) r else l)

    if (maxLocation + 1 - minLocation > k) false else true
  }

  /**
   * Check whether all pointers are not null
   * @param pointers
   * @return true if all pointers are not null
   */
  private def allPointerNotNull(pointers: Array[Posting]) = {
    pointers.foldLeft(true)((isNotNull, posting) => {
      isNotNull && posting != null
    })
  }

  /**
   * Does not implement OR operator here
   * @param invertedLists Inverted lists to be merged by OR
   * @return Resulting InvertedList (with scores)
   */
  protected def or(invertedLists: List[InvertedList]): InvertedList = {
    throw new UnsupportedOperationException("[%s] class did not implement unordered window method!".format(this.getClass.getSimpleName))
  }


}
