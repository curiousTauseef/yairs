package yairs.retrieval

import yairs.model.{InvertedList, QueryOperator, QueryTreeNode}

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 3/17/13
 * Time: 8:16 PM
 */
trait StructuredRetriever extends Retriever {
  protected def evaluateStructuredQuery(node: QueryTreeNode): InvertedList = {
    if (node.isLeaf) {
      getInvertedFile(node)
    }
    else {
      val childLists = node.children.foldLeft(List[InvertedList]())((lists, child) => {
        if (child.isStop)
          lists
        else
          evaluateStructuredQuery(child) :: lists
      }).reverse //ensure evaluation sequences
      val optimizedChildLists = if (node.operator == QueryOperator.AND) childLists.sortBy(l => l.postings.length) else childLists
      //mergePostingLists(optimizedChildLists, node)
      mergeNodes(optimizedChildLists, node)
    }
  }

  def mergeNodes(invertedLists: List[InvertedList], node: QueryTreeNode): InvertedList = {
    if (node.isLeaf) {
      throw new IllegalArgumentException("No intersection to do on leaf node")
      System.exit(1)
    }

    var isFirst = true //basically avoid empty list to enter conjunction operation

    if (node.operator == QueryOperator.UW) {
      unorderedWindow()
    } else invertedLists.foldLeft(InvertedList.empty())((mergingList, currentList) => {
      if (isFirst) {
        isFirst = false
        currentList
      }
      else {
        intersect2PostingLists(mergingList, currentList, node)
      }
    })
  }

  /**
   * The one-term-at-at-time implementation.
   * @param list1 The list to be intersect
   * @param list2 The other list to be intersect
   * @param node  The node where two list to be intersected at, which defines the query operator
   * @return Resulting posting list
   */
  def intersect2PostingLists(list1: InvertedList, list2: InvertedList, node: QueryTreeNode): InvertedList = {
    if (node.isLeaf) throw new IllegalArgumentException("No intersection to do on leaf node")

    if (node.operator == QueryOperator.AND) {
      conjunct(list1, list2)
    } else if (node.operator == QueryOperator.OR) {
      disjunct(list1, list2)
    } else if (node.operator == QueryOperator.NEAR) {
      twoWayMerge(list1, list2, node.proximity)
    } else {
      throw new IllegalArgumentException("The operator [%s] is not supported".format(node.operator))
      null
    }
  }

  protected def unorderedWindow(): InvertedList

  /**
   * Interesect two list based on postions and retain sequential. In other words, the "NEAR" operator
   * @param list1 The list to be intersect
   * @param list2 The other list to be intersect
   * @param k The proximity distance allowed
   * @return  Resulting posting list
   */
  protected def twoWayMerge(list1: InvertedList, list2: InvertedList, k: Int): InvertedList

  /**
   * OR operation for 2 posting lists intersection
   * @param list1
   * @param list2
   * @return
   */
  protected def disjunct(list1: InvertedList, list2: InvertedList): InvertedList

  /**
   * AND operation for 2 postings lists intersection
   * @param list1
   * @param list2
   * @return  Merged posting list
   */
  protected def conjunct(list1: InvertedList, list2: InvertedList): InvertedList
}
