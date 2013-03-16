package yairs.model

/**
 * Created with IntelliJ IDEA.
 * User: Hector
 * Date: 2/21/13
 * Time: 2:04 AM
 * To change this template use File | Settings | File Templates.
 */
class Posting(val docId: Int, val tf: Int, val length: Int, val positions: List[Int], val score: Double, val isEmpty :Boolean = false) {
}

object Posting {
  def apply(docId:Int,score:Double): Posting ={
     new Posting(docId,-1,-1,List[Int](),score)
  }

  def apply(docId:Int,positions:List[Int],score:Double): Posting ={
    new Posting(docId,-1,-1,positions,score)
  }
}
