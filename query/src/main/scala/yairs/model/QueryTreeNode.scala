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
  val defaultOperator = QueryOperator.OR
  val defaultField = "body"

  val queryString = subQuery.trim

  val operator = if (queryOperator == "#AND") QueryOperator.AND
  else if (queryOperator.startsWith("#NEAR")) QueryOperator.NEAR
  else defaultOperator

  val proximity = if (operator == QueryOperator.NEAR) queryOperator.split("/")(1).toInt else null

  private val subStringParts = PrefixBooleanQueryParser.split(queryString)

  val isLeaf = subStringParts.length == 1

  val children = if (isLeaf) null else subStringParts.map(part => PrefixBooleanQueryParser.parseNode(part))

  val (term, field) = if (isLeaf) {
    val parts = queryString.split('+')
    if (parts.length == 2) {
      (parts(0), parts(1))
    } else {
      (parts(0), defaultField)
    }
  } else (null, null)

  val isStop = if (isLeaf) PrefixBooleanQueryParser.isStop(term) else null

  def dump(layer: Int) {
    (1 to layer) foreach (_ => print("\t"))
    if (!isLeaf) {
      if (operator == QueryOperator.NEAR) println(operator + " "+ proximity)else println(operator)
    } else println(term + " : [" + field + "] " + " stop : " + isStop)
  }

}