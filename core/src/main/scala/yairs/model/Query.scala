package yairs.model

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/6/13
 * Time: 9:31 PM
 */
trait Query {
  val queryId: String
  val queryString: String
  val queryRoot:QueryTreeNode
  def dump()
}
