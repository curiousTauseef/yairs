package yairs.util

import yairs.model.QueryTreeNode
import org.eintr.loglady.Logging
import collection.mutable

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/20/13
 * Time: 11:19 AM
 */
object PrefixBooleanQueryParser extends QueryParser with Logging {
  def parseNode(str: String): QueryTreeNode = {
    if (str.startsWith("#OR")) {
      new QueryTreeNode("or", stripOuterBrackets(str.stripPrefix("#OR")))
    } else if (str.startsWith("#AND")) {
      new QueryTreeNode("and", stripOuterBrackets(str.stripPrefix("#AND")))
    } else if (str.startsWith("#NEAR")) {
      new QueryTreeNode("near", stripOuterBrackets(str.stripPrefix("#NEAR")))
    } else {
      new QueryTreeNode("", stripOuterBrackets(str))
    }
  }


  def split(subQuery: String): List[String] = {
    val strStack = new StringBuilder
    val bracketStack = new mutable.Stack[Char]

    log.debug(subQuery)

    val subNodeStrs = subQuery.foldLeft(List[String]())((strs, char) => {
      if (char == '(') {
        bracketStack.push(char)
      }
      if (char == ')') {
        bracketStack.pop()
      }

      if (char == ' ' && bracketStack.isEmpty && !isOperator(strStack.toString().trim)) {
        val newStrs = strs ::: List(strStack.toString())
        strStack.clear()
        newStrs
      }
      else{
        strStack.append(char)
        strs
      }
    }) ::: List(strStack.toString())

    subNodeStrs
  }

  def isOperator(str: String) = (str == "#AND" || str == "#OR" || str == "#NEAR")

  def stripOuterBrackets(str: String):String = {
    val trimmed = str.trim
    if (trimmed.startsWith("(")&&trimmed.endsWith(")")) {
      trimmed.stripPrefix("(").stripSuffix(")")
    }else{
      trimmed
    }
  }
}
