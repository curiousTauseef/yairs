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
//  private val defaultOperator = QueryOperator.AND
  private val defaultField = QueryField.BODY

  private val queryString = subQuery.trim

  val operator = if (queryOperator == "#AND") QueryOperator.AND
  else if (queryOperator.startsWith("#NEAR")) QueryOperator.NEAR
  else if (queryOperator == "#OR") QueryOperator.OR
  else throw new IllegalArgumentException("Cannot recognize query operator")

  val proximity = if (operator == QueryOperator.NEAR) queryOperator.split("/")(1).toInt else 1

  private val subStringParts = PrefixBooleanQueryParser.split(queryString)

  //the lower fields only make sense when it is a leaf
  val isLeaf = subStringParts.length == 1

  val children = if (isLeaf) null else subStringParts.map(part => PrefixBooleanQueryParser.parseNode(part))

  val (term, field) = if (isLeaf) {
    val parts = queryString.split('+')
    if (parts.length == 2) {
      val fieldStr = parts(1)
      val field = if (fieldStr == "body") QueryField.BODY else if (fieldStr == "title") QueryField.TITLE else defaultField
      (parts(0), field)
    } else {
      (parts(0), defaultField)
    }
  }else ("",defaultField)

  val isStop = if (isLeaf) PrefixBooleanQueryParser.isStop(term) else false

  private[model] def dump(layer: Int) {
    (1 to layer) foreach (_ => print("\t"))
    dump()
  }

  def dump(){
    if (!isLeaf) {
      if (operator == QueryOperator.NEAR) println(operator + " "+ proximity)else println(operator)
    } else println(term + " : [" + field + "] " + " stopword : " + isStop)
  }
}