package yairs.retrieval

import yairs.model._
import yairs.io.BooleanQueryReader
import java.io.File
import yairs.util.FileUtil

/**
 * Created with IntelliJ IDEA.
 * User: Hector
 * Date: 2/21/13
 * Time: 12:56 AM
 * To change this template use File | Settings | File Templates.
 */
class BooleanRetriever extends Retriever{
  def evaluate(query: Query):List[Result] = {
     val bQuery = query.asInstanceOf[BooleanQuery]
     val root = bQuery.queryRoot
     postings2Results(evaluateNode(root))
  }

  private def postings2Results(postings:List[Posting]):List[Result] ={
    null
  }

  private def evaluateNode(node:QueryTreeNode):List[Posting] = {
     if (node.isLeaf){
       InvertedList(FileUtil.getInvertedFile(node.term)).postings
     }else{
       val childLists = node.children.foldLeft(List[List[Posting]]())((lists,child)=>{
         evaluateNode(child) :: lists
       })
       mergePostingLists(childLists,node)
     }
  }

  private def mergePostingLists(postingLists :List[List[Posting]],node:QueryTreeNode):List[Posting] = {
    postingLists.foldLeft(List[Posting]())((mergingList,currentList) =>{
       merge2PostingList(mergingList,currentList,node)
  })
  }

  private def merge2PostingList(list1:List[Posting],list2:List[Posting],node:QueryTreeNode):List[Posting] = {
    if (node.operator == QueryOperator.AND){

    } else if (node.operator == QueryOperator.OR){

    } else if (node.operator == QueryOperator.NEAR){
       val proximity = node.proximity
    }
    null//wait
  }
}

object BooleanRetriever{
  def main(args:Array[String]){
    val qr = new BooleanQueryReader()
    val queries = qr.getQueries(new File("data/queries.txt"))
    queries.foreach(query =>{
       val br = new BooleanRetriever()
       br.evaluate(query)
    })
  }
}
