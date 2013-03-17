package yairs.model

import yairs.util.PrefixQueryParser
import org.eintr.loglady.Logging

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/20/13
 * Time: 10:36 AM
 */
class QueryTreeNode(val queryOperator: String, subQuery: String, val defaultField: String ,queryPaser: PrefixQueryParser) extends Logging {
  private val queryString = subQuery.trim

  val operator = if (queryOperator == "#AND") QueryOperator.AND
  else if (queryOperator.startsWith("#NEAR")) QueryOperator.NEAR
  else if (queryOperator == "#OR") QueryOperator.OR
  else if (queryOperator == "#SUM") QueryOperator.SUM
  else throw new IllegalArgumentException("Cannot recognize query operator")

  val proximity = if (operator == QueryOperator.NEAR) queryOperator.split("/")(1).toInt else 1

  private val subStringParts = queryPaser.split(queryString).filterNot(token => containsNoLetter(token))

  def containsNoLetter(str: String): Boolean = {
    str.foreach(ch => {
      if (ch.isLetter) return false
    })
    true
  }

  //the lower fields only make sense when it is a leaf
  val isLeaf = subStringParts.length == 1

  val children = if (isLeaf) null else subStringParts.map(part => queryPaser.parseQueryString(part))

  val (term, field) = if (isLeaf) {
    val parts = queryString.split('+')
    if (parts.length == 2) {
      (parts(0), parts(1))
    } else {
      (parts(0), defaultField)
    }
  } else ("", defaultField)

  val isStop = if (isLeaf) queryPaser.isStop(term) else false

  private[model] def dump(layer: Int) {
    (1 to layer) foreach (_ => print("\t"))
    dump()
  }

  def dump() {
    if (!isLeaf) {
      if (operator == QueryOperator.NEAR) println(operator + " " + proximity) else println(operator)
    } else println(term + " : [" + field + "] " + " stopword : " + isStop)
  }
}