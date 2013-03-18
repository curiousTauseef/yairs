package yairs.retrieval

import yairs.model.{InvertedList, Posting, QueryTreeNode}
import collection.mutable

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 3/17/13
 * Time: 8:16 PM
 */
trait BOWRetriever extends Retriever {
  protected def evaluateBowQuery(node: QueryTreeNode): List[Posting] = {
    if (node.isLeaf)
      bowEvaluate(List(node))
    else
      bowEvaluate(node.children)
  }

  def bowEvaluate(nodes: List[QueryTreeNode]): List[Posting] = {
    val documentScores = new mutable.HashMap[Int, Double]()

    nodes.foreach(node => {
      if (!node.isLeaf) {
        throw new UnsupportedOperationException("Bag of word retriever does not support structured queries")
        System.exit(1)
      }
      val invertedList = getInvertedFile(node)
      invertedList.postings.foreach(posting => {
        val docId = posting.docId
        //val score = termScorer(invertedList.collectionFrequency,invertedList.totalTermCount,invertedList.documentFrequency,posting.tf,posting.docLength)
        if (documentScores.contains(docId)) {
          documentScores(docId) += posting.score
        } else {
          documentScores(docId) = posting.score
        }
      }
      )
    })

    documentScores.toList.map {
      case (docId, score) => Posting(docId, score)
    }.toList
  }
}
