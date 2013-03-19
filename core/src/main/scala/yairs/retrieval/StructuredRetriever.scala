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

  def mergeNodes(invertedLists: List[InvertedList], node: QueryTreeNode): InvertedList
}
