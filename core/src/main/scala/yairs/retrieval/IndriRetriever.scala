package yairs.retrieval

import yairs.model._
import yairs.util.{Configuration, FileUtils}
import collection.mutable.ListBuffer
import util.control.Breaks._
import org.eintr.loglady.Logging

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 3/17/13
 * Time: 4:28 PM
 */
class IndriRetriever(config: Configuration) extends BOWRetriever with StructuredRetriever with Logging {
  val name: String = "Indri"

  val isStructured = config.getBoolean("yairs.query.isStructure")

  val invBaseName = config.get("yairs.inv.basename")
  val documentCount = config.getInt("yairs.document.count").toDouble
  val totalWordCount = config.getInt("yairs.word.count").toDouble

  /**
   * Wrapper to get the results
   * @param query
   * @param runId
   * @return
   */
  def getResults(query: Query, runId: String): List[Result] = {
    evaluate(query, runId, sortById = false, isStructured)
  }

  protected def getInvertedFile(node: QueryTreeNode, scorer: (Int, Int, Int, Int) => Double = termScorer): InvertedList = {
    log.debug("Getting inverted file for [%s]".format(node.term))
    InvertedList(FileUtils.getInvertedFile(invBaseName: String, node.term, node.field, node.defaultField, isHw2 = true), scorer, config)
  }

  protected def termScorer(collectionFrequency: Int, documentFreq: Int, termFrequency: Int, documentLength: Int): Double = {
    val lambda = 0.4
    val mu = 2500.0

    val collectionPrior = collectionFrequency / totalWordCount
    //val collectionPrior = documentFreq/documentCount
    val weight = lambda * (termFrequency + mu * collectionPrior) / (documentLength + mu) + (1 - lambda) * collectionPrior

    if (weight < 0) {
      println("fuck")
      println(termFrequency, collectionFrequency, documentLength, collectionPrior)
      System.exit(1)
    }
    //    println(termFrequency+" "+documentLength)
    //    println(collectionPrior+" "+weight+" "+lambda*(termFrequency + mu*collectionPrior)/(documentLength+mu))
    math.log10(weight)
  }

  override def mergeNodes(invertedLists: List[InvertedList], node: QueryTreeNode): InvertedList = {
    if (node.isLeaf) throw new IllegalArgumentException("No intersection to do on leaf node")

    if (node.operator == QueryOperator.AND) {
      multiwayMerge(invertedLists, List.fill(invertedLists.length)(1))
    } else if (node.operator == QueryOperator.NEAR) {
      var isFirst = true //basically avoid empty list to enter conjunction operation
      invertedLists.foldLeft(InvertedList.empty())((mergingList, currentList) => {
        if (isFirst) {
          isFirst = false
          currentList
        }
        else {
          near(mergingList, currentList, node.proximity)
        }
      })
    } else if (node.operator == QueryOperator.WEIGHT) {
      multiwayMerge(invertedLists, node.weights)
    } else if (node.operator == QueryOperator.UW) {
      unorderedWindow(invertedLists, node.proximity)
    } else {
      throw new IllegalArgumentException("The operator [%s] is not supported".format(node.operator))
      null
    }

  }

  private def unorderedWindow(invertedLists: List[InvertedList], k: Int): InvertedList = {
    val iters = invertedLists.map(list => list.postings.iterator).toArray
    val intersectedPostings = new ListBuffer[Posting]()

    val pointers = iters.map(iter => if (iter.hasNext) iter.next() else null)

    val weightedDefaultScores = invertedLists.map(list => list.defaultScore)
    val combinedDefaultScores = weightedDefaultScores.foldLeft(0.0)((comb, ws) => comb + ws)

    var totalCollectionFreq = 0
    while (allPointerNotNull(pointers)) {
      val docIds = pointers.map(pointer => pointer.docId)
      val minDocId = docIds.reduceLeft((l, r) => if (r < l) r else l)

      val isSameDoc = docIds.foldLeft(true)((isSame, r) => (r == minDocId) && isSame)

      val uwTermFreq = if (isSameDoc) {
        //log.debug("Is same doc:"+minDocId)
        val positionsList = pointers.map(pointer => pointer.positions)
        checkUWPositions(positionsList, k)
      } else 0 //0 means either no match, or these three terms does not has the same document

      totalCollectionFreq += uwTermFreq

      //combine all pointers score
      //there are no weights in UW
      //TODO: but there is a problem of how to define the score for not-matched UW
      //we alwasy want to give them some non-zeor scores (we are in Log scalse, so non-negative-infinite scores)
      val score = if (uwTermFreq > 0) pointers.foldLeft(0.0)((s, pointer) => pointer.score + s) else combinedDefaultScores

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
      intersectedPostings.append(Posting(minDocId, score))
    }

    val totalDocumentFreq = intersectedPostings.foldLeft(0)((sum, posting) => {
      sum + posting.docLength
    })

    //We use the total document frequency amongst the postings as the document frequency
    //We sum up the term frequency of each UW amongst the inverted lists as the collection frequency
    InvertedList(totalCollectionFreq, invertedLists(0).totalTermCount, totalDocumentFreq, intersectedPostings.toList, combinedDefaultScores)
  }

  private def checkUWPositions(positionsList: Iterable[List[Int]], k: Int): Int = {
    val iters = positionsList.map(list => list.iterator).toArray
    val itersWithIndex = iters.zipWithIndex

    var numMatch = 0

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

  private def allValidLocations(positions: Iterable[Int]): Boolean = {
    positions.foldLeft(true)((isValid, position) => {
      isValid && !(position < 0)
    })
  }

  private def matchUWCondition(locations: Iterable[Int], k: Int): Boolean = {
    val minLocation = locations.reduceLeft((l, r) => if (r < l) r else l)
    val maxLocation = locations.reduceLeft((l, r) => if (r > l) r else l)

    if (maxLocation + 1 - minLocation > k) false else true
  }

  /**
   * Interesect two list based on postions and retain sequential. In other words, the "NEAR" operator
   * @param list1 The list to be intersect
   * @param list2 The other list to be intersect
   * @param k The proximity distance allowed
   * @return  Resulting posting list
   */
  protected def near(list1: InvertedList, list2: InvertedList, k: Int): InvertedList = {
    val iter1 = list1.postings.iterator
    val iter2 = list2.postings.iterator

    val defaultScore = list1.defaultScore + list2.defaultScore

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
              val score = p1.score + p2.score
              intersectedPostings.append(Posting(docId1, nearMatchesList.map(_._2), score))
            }
            if (!(iter1.hasNext && iter2.hasNext)) {
              break()
            }
            p1 = iter1.next()
            p2 = iter2.next()
          } else if (docId1 < docId2) {
            //we still give default socre if no near is found, but an empty positions list
            intersectedPostings.append(Posting(docId1, List(), defaultScore))
            if (!iter1.hasNext) break()
            p1 = iter1.next()
          } else {
            //we still give default socre if no near is found, but an empty positions list
            intersectedPostings.append(Posting(docId2, List(), defaultScore))
            if (!iter2.hasNext) break()
            p2 = iter2.next()
          }
        }
      }
    }
    InvertedList(collectionFreq, list1.totalTermCount, documentFreq, intersectedPostings.toList, defaultScore)
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

  /**
   * AND operation for multiple postings lists intersection
   *
   * @return  Merged posting list
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

    //val minDocumentFreq = invertedLists.foldLeft(Integer.MAX_VALUE)((minDocumentFreq,list)=>{if (list.documentFrequency<minDocumentFreq) list.documentFrequency else minDocumentFreq})
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

  private def allPointerNotNull(pointers: Array[Posting]) = {
    pointers.foldLeft(true)((isNotNull, posting) => {
      isNotNull && posting != null
    })
  }

  private def somePointerNotNull(pointers: Array[Posting]) = {
    pointers.foldLeft(false)((isNull, posting) => {
      isNull || posting != null
    })
  }

}