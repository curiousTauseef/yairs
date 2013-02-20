package yairs.model

import yairs.util.PrefixBooleanQueryParser

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/20/13
 * Time: 10:36 AM
 */
class QueryTree(queryString: String) {
  val root = PrefixBooleanQueryParser.parseNode(queryString)

}
