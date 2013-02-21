package yairs.model

import yairs.util.PrefixBooleanQueryParser
import org.eintr.loglady.Logging
import io.Source

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/20/13
 * Time: 10:36 AM
 */
class QueryTreeNode(val queryOperator: String, val subQuery: String) extends Logging {

  object QueryOperator extends Enumeration {
    type QueryOperator = Value
    val OR, AND, NEAR = Value
  }

  val defaultOperator = QueryOperator.OR
  val defaultField = "body"

  val queryString = subQuery.trim

  val operator = if (queryOperator == "and") QueryOperator.AND
  else if (queryOperator == "near") QueryOperator.NEAR
  else defaultOperator

  private val subStringParts = PrefixBooleanQueryParser.split(queryString)

  val isLeaf = subStringParts.length == 1

  val children = if (isLeaf) null else subStringParts.map(part => PrefixBooleanQueryParser.parseNode(part))

  val (term, field) = if(isLeaf) {
    val parts = queryString.split('+')
    if (parts.length == 2) {
      (parts(0), parts(1))
    } else {
      (parts(0), defaultField)
    }
  } else (null,null)

  val isStop = if (isLeaf) PrefixBooleanQueryParser.isStop(term) else null

  def dump(layer: Int) {
    (1 to layer) foreach (_ => print("\t"))
    if (!isLeaf) println(operator) else println(term + " : ["+ field+"] "+isStop)
  }

}