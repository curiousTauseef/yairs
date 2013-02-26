package yairs.util

import yairs.model.QueryTreeNode
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
class PrefixBooleanQueryParser (stopWordFile:File) extends QueryParser with Logging {
  private val stopWordDict = Source.fromFile(stopWordFile).getLines().toSet
  //private val defaultOperator = "#AND"

  def isStop(word:String) = stopWordDict.contains(word.trim)

  def parseQueryString(rawStr: String,defaultOperator: String): QueryTreeNode = {
    val str = rawStr.trim
//    println(str)
    if (str.startsWith("#OR")) {
      new QueryTreeNode("#OR", stripOuterBrackets(str.stripPrefix("#OR")),defaultOperator,this)
    } else if (str.startsWith("#AND")) {
      new QueryTreeNode("#AND", stripOuterBrackets(str.stripPrefix("#AND")),defaultOperator,this)
    } else if (str.startsWith("#NEAR")) {
      val reg = """^(#NEAR/\d+)(.*)""".r
      val  reg(prefix,suffix) = str
      new QueryTreeNode(prefix, stripOuterBrackets(suffix),defaultOperator,this)
    } else {
      new QueryTreeNode(defaultOperator, stripOuterBrackets(str),defaultOperator,this)
    }
  }

  def split(subQuery: String): List[String] = {
    val strBuffer = new StringBuilder
    val bracketStack = new mutable.Stack[Char]

    var subNodeStrBuffer = ListBuffer.empty[String]

   subQuery.foreach(char =>{
      if (char == '(') {
        bracketStack.push(char)
      }
      if (char == ')') {
        bracketStack.pop()
      }

      if (char == ' ' && bracketStack.isEmpty && !isOperator(strBuffer.toString().trim)) {
        subNodeStrBuffer += strBuffer.toString()
        strBuffer.clear()
      }
      else{
        strBuffer.append(char)
      }
    })
    subNodeStrBuffer += strBuffer.toString()
    subNodeStrBuffer.toList
  }

  def isOperator(str: String):Boolean = {
    if (str == "#AND" || str == "#OR") true
    else{
      val reg = """^(#NEAR/\d+)(.*)""".r
      reg findFirstIn str match{
        case Some(reg(prefix,suffix)) => suffix == ""
        case None => false
      }
    }
  }

  def stripOuterBrackets(str: String):String = {
    val trimmed = str.trim
    if (trimmed.startsWith("(")&&trimmed.endsWith(")")) {
      stripOuterBrackets(trimmed.stripPrefix("(").stripSuffix(")"))
    }else{
      trimmed
    }
  }
}
