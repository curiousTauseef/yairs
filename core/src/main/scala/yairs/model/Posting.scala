package yairs.model

/**
 * Created with IntelliJ IDEA.
 * User: Hector
 * Date: 2/21/13
 * Time: 2:04 AM
 * To change this template use File | Settings | File Templates.
 */
class Posting(val docId: Int, val tf: Int, val length: Int, val positions: List[Int], val score: Float = 0.0.toFloat) {
  val isEmpty = positions.length == 0
}

object Posting {
  def empty(): Posting = {
    new Posting(-1, -1, -1, List[Int]())
  }
}
