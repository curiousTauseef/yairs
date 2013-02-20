package yairs.model

import yairs.util.PrefixBooleanQueryParser
import org.eintr.loglady.Logging

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/6/13
 * Time: 9:35 PM
 */
class BooleanQuery(id: String, query: String) extends Query with Logging {
  val queryId = id
  val queryString = query

  val queryRoot = PrefixBooleanQueryParser.parseNode(queryString)

  def dump() {
    println("Query Id: " + queryId)
    dumpQueryTree(queryRoot, 0)
  }

  def dumpQueryTree(node: QueryTreeNode, layer: Int) {
    node.dump(layer)
    if (!node.isLeaf) {
      node.children.foreach(child => dumpQueryTree(child, layer + 1))
    }
  }
}
