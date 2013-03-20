package yairs.retrieval

import yairs.model.{Posting, InvertedList, QueryOperator, QueryTreeNode}
import collection.mutable.ListBuffer
import yairs.util.Configuration
import util.control.Breaks._

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 3/17/13
 * Time: 8:16 PM
 */
trait StructuredRetriever extends Retriever {
  /**
   * Implement the core method for Retriever, evaluate a query recursively
   * This method simply retrieve the Inverted List of the nodes, then do a simple optimization,
   * which is sort them by posting length, and put them into the next pipeline to distribute
   * the lists to specific operator
   * @param node The query node given
   * @param config The configuration of the system
   * @return  Resulting InvertedList (with scores)
   */
  protected def evaluateQuery(node: QueryTreeNode, config:Configuration): InvertedList = {
    if (node.isLeaf) {
      getInvertedFile(node)
    }
    else {
      val childLists = node.children.foldLeft(List[InvertedList]())((lists, child) => {
        if (child.isStop)
          lists
        else
          evaluateQuery(child,config) :: lists
      }).reverse //ensure evaluation sequences
      val optimizedChildLists = if (node.operator == QueryOperator.AND) childLists.sortBy(l => l.postings.length) else childLists
      //mergePostingLists(optimizedChildLists, node)
      mergeNodes(optimizedChildLists, node,config)
    }
  }

  /**
   * Choose what method to call based on the Operator
   * @param invertedLists The inverted lists to be merged
   * @param node The query node, mainly used to get the operator type
   * @param config The configuration of the system
   * @return  Resulting InvertedList (with scores)
   */
  def mergeNodes(invertedLists: List[InvertedList], node: QueryTreeNode,config:Configuration): InvertedList = {
    if (node.isLeaf) throw new IllegalArgumentException("No intersection to do on leaf node")
    if (node.operator == QueryOperator.OR){
      or(invertedLists)
    }else if (node.operator == QueryOperator.AND || node.operator == QueryOperator.SUM) {
      and(invertedLists)
    } else if (node.operator == QueryOperator.NEAR) {
      var isFirst = true //basically avoid empty list to enter conjunction operation
      invertedLists.foldLeft(InvertedList.empty())((mergingList, currentList) => {
        if (isFirst) {
          isFirst = false
          currentList
        }
        else {
          near(mergingList, currentList, node.proximity,config)
        }
      })
    } else if (node.operator == QueryOperator.WEIGHT) {
      weight(invertedLists, node.weights)
    } else if (node.operator == QueryOperator.UW) {
      unorderedWindow(invertedLists, node.proximity,termScorer, config)
    } else {
      throw new IllegalArgumentException("The operator [%s] is not supported".format(node.operator))
      null
    }
  }

  /**
   * Subclasses need to implement this
   * @param invertedLists Inverted lists to be merged by OR
   * @return Resulting InvertedList (with scores)
   */
  protected def or(invertedLists:List[InvertedList]):InvertedList

  /**
   * Sub classes need to implement this
   * @param invertedLists Inverted lists to be merged by AND
   * @return Resulting InvertedList (with scores)
   */
  protected def and(invertedLists:List[InvertedList]):InvertedList

  /**
   * Leave this unimpelementd. Subclasses could implement this
   * @param invertedLists Inverted lists to be merged using WEIGHT
   * @param weights weights of of each list
   * @return Resulting Inverted list(with score)
   */
  protected def weight(invertedLists:List[InvertedList], weights:List[Double]):InvertedList


  /**
   * Sub classes will implement this
   * This will result in a virtual term, thus scoring function need to be provided
   * @param invertedLists Inverted list to be merged using UW
   * @param k  Proximity
   * @param scorer How to score a term
   * @param config The configuration of the system
   * @return
   */
  protected def unorderedWindow(invertedLists:List[InvertedList], k:Int, scorer:(Int,Int,Int,Int)=>Double, config:Configuration):InvertedList


  /**
   * Near operator, go through two document list, and check near in each the position lists
   * Score are calculated by considering one near match as one term occurrence, thus Collection freq and Term freq can be
   * calculated respectively
   * @param list1
   * @param list2
   * @param k
   * @param config
   * @return
   */
  protected def near(list1:InvertedList,list2:InvertedList,k:Int, config:Configuration):InvertedList = {
    val iter1 = list1.postings.iterator
    val iter2 = list2.postings.iterator

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
              intersectedPostings.append(new Posting(docId1, matches,p1.docLength,nearMatchesList.map(_._2),0))
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

    val scoredPostings = intersectedPostings.map(posting =>{
      val score = termScorer(collectionFreq,documentFreq,posting.tf,posting.docLength)
      new Posting(posting.docId,posting.tf,posting.docLength,posting.positions,score)
    }).toList

    InvertedList(collectionFreq, list1.totalTermCount, documentFreq, scoredPostings, termScorer,config)
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
}
