package yairs.util

import yairs.model.{QueryOperator, QueryField, QueryTreeNode}
import org.eintr.loglady.Logging
import collection.mutable
import io.Source
import collection.mutable.ListBuffer
import java.io.File

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/20/13
 * Time: 11:19 AM
 */
class PrefixQueryParser(config:Configuration) extends QueryParser with Logging {
  val defaultField = config.get("yairs.field.default")
  val defaultOperator = config.getDefaultOperator("yaris.operator.default")
  val stopWordFilePath = config.get("yairs.stoplist.path")
  val stopWordFile = new File(stopWordFilePath)
  private val stopWordDict = Source.fromFile(stopWordFile).getLines().toSet

  def isStop(word: String) = stopWordDict.contains(word.trim)

  def parseQueryString(rawStr: String): QueryTreeNode = {
    val str = rawStr.trim.toLowerCase()
    if (str.startsWith("#or")) {
      new QueryTreeNode(QueryOperator.OR,0, stripOuterBrackets(str.stripPrefix("#or")),defaultField, this)
    } else if (str.startsWith("#and")) {
      new QueryTreeNode(QueryOperator.AND,0, stripOuterBrackets(str.stripPrefix("#and")),defaultField, this)
    } else if (str.startsWith("#near")) {
      val reg = """^(#near/\d+)(.*)""".r
      val reg(prefix, suffix) = str
      new QueryTreeNode(QueryOperator.NEAR,prefix.split("/")(1).toInt, stripOuterBrackets(suffix),defaultField, this)
    } else if (str.startsWith("#sum")){
      new QueryTreeNode(QueryOperator.SUM,0,stripOuterBrackets(str.stripPrefix("#sum")),defaultField,this)
    } else if (str.startsWith("#weight")){
      new QueryTreeNode(QueryOperator.WEIGHT,0,stripOuterBrackets(str.stripPrefix("#weight")),defaultField,this)
    } else if (str.startsWith("#uw")){
      val reg = """^(#uw/\d+)(.*)""".r
      val reg(prefix, suffix) = str
      new QueryTreeNode(QueryOperator.UW,prefix.split("/")(1).toInt, stripOuterBrackets(suffix),defaultField, this)
    }else {
      new QueryTreeNode(defaultOperator,0, stripOuterBrackets(str),defaultField, this)
    }
  }

  def split(subQuery: String): List[String] = {
    val strBuffer = new StringBuilder
    val bracketStack = new mutable.Stack[Char]

    var subNodeStrBuffer = ListBuffer.empty[String]

    subQuery.foreach(char => {
      if (char == '(') {
        bracketStack.push(char)
      }
      if (char == ')') {
        bracketStack.pop()
      }

      if ((char == ' ' || char == '-') && bracketStack.isEmpty && !isOperator(strBuffer.toString().trim)) {
        subNodeStrBuffer += strBuffer.toString()
        strBuffer.clear()
      }
      else {
        strBuffer.append(char)
      }
    })
    subNodeStrBuffer += strBuffer.toString()
    subNodeStrBuffer.toList
  }

  def isOperator(str: String): Boolean = {
    if (str == "#AND" || str == "#OR") true
    else {
      val reg = """^(#NEAR/\d+)(.*)""".r
      reg findFirstIn str match {
        case Some(reg(prefix, suffix)) => suffix == ""
        case None => false
      }
    }
  }

  def stripOuterBrackets(str: String): String = {
    val trimmed = str.trim
    if (trimmed.startsWith("(") && trimmed.endsWith(")")) {
      stripOuterBrackets(trimmed.stripPrefix("(").stripSuffix(")"))
    } else {
      trimmed
    }
  }
}
