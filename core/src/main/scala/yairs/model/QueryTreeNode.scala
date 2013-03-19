package yairs.model

import yairs.util.PrefixQueryParser
import org.eintr.loglady.Logging

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/20/13
 * Time: 10:36 AM
 */
class QueryTreeNode(val operator: QueryOperator.Value, val operatorOverHead: Int, subQuery: String, val defaultField: String, queryPaser: PrefixQueryParser) extends Logging {
  private val queryString = subQuery.trim

  val proximity = if (operator == QueryOperator.NEAR || operator == QueryOperator.UW) operatorOverHead else 1

  val (isLeaf, children, weights) =
    if (operator == QueryOperator.WEIGHT) {
      val subStringParts = queryPaser.split(queryString).filter(token => containsNoLetter(token)).grouped(2).toList
      val unNormalizedWeights = subStringParts.map(group => group(0).toDouble)
      val weightSum = unNormalizedWeights.sum
      val weights = unNormalizedWeights.map(weight => weight/weightSum).toList
      val children = subStringParts.map(group => queryPaser.parseQueryString(group(1))).toList
      (false,children,weights)
    } else {
      val subStringParts = queryPaser.split(queryString).filter(token => containsNoLetter(token))
      val isLeaf = subStringParts.length == 1
      val children = if (isLeaf) null else subStringParts.map(part => queryPaser.parseQueryString(part))
      (isLeaf,children,null)
    }

  def containsNoLetter(str: String): Boolean = {
    str.foreach(ch => {
      if (ch.isLetterOrDigit)
        false
    })
    true
  }


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
      if (operator == QueryOperator.NEAR || operator == QueryOperator.UW) println(operator + " " + proximity)
      else if (operator == QueryOperator.WEIGHT) {
        print(operator+ "\t")
        weights.foreach(weight => print(weight + " "))
        println()
      }
      else println(operator)
    } else println(term + " : [" + field + "] " + " stopword : " + isStop)
  }
}