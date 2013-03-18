package yairs.retrieval

import yairs.model._
import yairs.util.{Configuration, FileUtils}
import collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 3/17/13
 * Time: 4:28 PM
 */
class IndriRetriever(config: Configuration) extends BOWRetriever with StructuredRetriever {
  val name: String = "Indri"

  val isStructured = config.getBoolean("yairs.query.isStructure")

  val invBaseName = config.get("yairs.inv.basename")
  val documentCount = config.getInt("yairs.document.count").toDouble
  val averageDocumentSize = config.getInt("yairs.document.average.size")
  val vocabularySize = config.getInt("yairs.vocabulary.size").toDouble
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
    InvertedList(FileUtils.getInvertedFile(invBaseName: String, node.term, node.field, node.defaultField, isHw2 = true), scorer)
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
    val iters = invertedLists.map(list => list.postings.iterator).toArray

    val intersectedPostings = new ListBuffer[Posting]()

    val defaultCollectionFreq = (totalWordCount / documentCount).toInt
    val defaultScore = termScorer(defaultCollectionFreq, 0, 0, averageDocumentSize)

    val pointers = iters.map(iter => if (iter.hasNext) iter.next() else null)

    while (checkAllPointers(pointers)) {
      val docIds = pointers.filterNot(_ == null).map(pointer => pointer.docId)
      val minDocId = docIds.reduceLeft((l, r) => if (r < l) r else l)
      var score = 0.0

      var index = 0
      pointers.foreach(pointer => {
        if (pointer != null && pointer.docId == minDocId) {
          score += pointer.score
          //          if (minDocId == 1 || minDocId == 889209) {
          //            println(minDocId)
          //            println("%s has it, add %s".format(index, pointer.score))
          //            println(pointer.docId)
          //        }
          if (iters(index).hasNext) {
            pointers(index) = iters(index).next()
          } else {
            pointers(index) = null
          }

        } else {
          score += defaultScore
        }

        index += 1
      })
      intersectedPostings.append(Posting(minDocId, score))
    }

    InvertedList(defaultCollectionFreq, invertedLists(0).totalTermCount, -1, intersectedPostings.toList)
  }


  def checkAllPointers(pointers: Array[Posting]) = {
    pointers.foldLeft(false)((isNull, posting) => {
      isNull || posting != null
    })
  }

  protected def unorderedWindow(): InvertedList = null

  /**
   * Interesect two list based on postions and retain sequential. In other words, the "NEAR" operator
   * @param list1 The list to be intersect
   * @param list2 The other list to be intersect
   * @param k The proximity distance allowed
   * @return  Resulting posting list
   */
  protected def twoWayMerge(list1: InvertedList, list2: InvertedList, k: Int): InvertedList = null

  /**
   * AND operation for 2 postings lists intersection
   * @param list1
   * @param list2
   * @return  Merged posting list
   */
  protected def conjunct(list1: InvertedList, list2: InvertedList): InvertedList = {
    throw new UnsupportedOperationException("2 Way merge does not work for Indri")
    //    val iter1 = list1.postings.iterator
    //    val iter2 = list2.postings.iterator
    //
    //    val intersectedPostings = new ListBuffer[Posting]()
    //
    //    val defaultCollectionFreq = (totalWordCount/documentCount).toInt
    //    val defaultScore = termScorer(defaultCollectionFreq,0,0,averageDocumentSize)
    //
    //    //totalTermCount is a duplicated statistic in each inverted list!
    //    var documentFreq = 0
    //    if (iter1.hasNext && iter2.hasNext) {
    //      var p1 = iter1.next()
    //      var p2 = iter2.next()
    //
    //      breakable {
    //        while (true) {
    //          val docId1 = p1.docId
    //          val docId2 = p2.docId
    //
    //          if (docId1 == docId2) {
    //            intersectedPostings.append(Posting(docId1, p1.score + p2.score))
    //            documentFreq += 1
    //            if (!(iter1.hasNext && iter2.hasNext)) {
    //              break()
    //            }
    //            p1 = iter1.next()
    //            p2 = iter2.next()
    //          } else if (docId1 < docId2) { //only appear in document 1
    //            intersectedPostings.append(Posting(docId1, defaultScore+p1.score))
    //            if (!iter1.hasNext) break()
    //            p1 = iter1.next()
    //          } else { //only appear in document 2
    //            intersectedPostings.append(Posting(docId2, defaultScore+p2.score))
    //
    //            if (!iter2.hasNext) break()
    //            p2 = iter2.next()
    //          }
    //        }
    //      }
    //    }
    //
    //    while (iter1.hasNext) {  //only appear in document one
    //      val p = iter1.next()
    //      intersectedPostings.append(Posting(p.docId, p.score + defaultScore))
    //    }
    //
    //    while (iter2.hasNext) { //only appear in document two
    //      val p = iter2.next()
    //      intersectedPostings.append(Posting(p.docId, p.score + defaultScore))
    //    }
    //
    //    InvertedList(-1, list1.totalTermCount, documentFreq,intersectedPostings.toList)
  }

  protected def disjunct(list1: InvertedList, list2: InvertedList): InvertedList = {
    throw new UnsupportedOperationException("OR is not defined for Indri retriever")
  }
}