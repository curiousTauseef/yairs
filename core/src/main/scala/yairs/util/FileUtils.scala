package yairs.util

import java.io.File

/**
 * Created with IntelliJ IDEA.
 * User: Hector
 * Date: 2/21/13
 * Time: 1:26 AM
 * To change this template use File | Settings | File Templates.
 */
object FileUtils {
  def getInvertedFile(invFileBaseName: String, term: String, field: String, isDefaultField: Boolean): File = {
    val realField = if (isDefaultField) "" else field
    val pathName = (invFileBaseName+realField).stripSuffix("_") + "/"
    val filePath = if (isDefaultField) pathName + term + ".inv" else pathName + term +"."+ field + ".inv"

    try {
      new File(filePath)
    } catch {
      case e: Exception => e.printStackTrace()
      throw new IllegalArgumentException("Cannot find InvertedList at [%s]. Maybe field [%s] is not supported".format(filePath,field))
      System.exit(1)
      null
    }
  }

  def getInvertedFile(invFileBaseName: String, term: String, field: String, isDefaultField: Boolean, isHw2:Boolean = true): File = {
    val pathName = invFileBaseName+"/"+field + "/"
    val filePath = if (isDefaultField) pathName + term + ".inv" else pathName + term +"."+ field + ".inv"

    try {
      new File(filePath)
    } catch {
      case e: Exception => e.printStackTrace()
      throw new IllegalArgumentException("Cannot find InvertedList at [%s]. Maybe field [%s] is not supported".format(filePath,field))
      System.exit(1)
      null
    }
  }
}
