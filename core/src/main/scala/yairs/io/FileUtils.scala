package yairs.io

import java.io.File

/**
 * Created with IntelliJ IDEA.
 * User: Hector
 * Date: 2/21/13
 * Time: 1:26 AM
 * To change this template use File | Settings | File Templates.
 */
object FileUtils {
//  def getInvertedFile(invFileBaseName: String, term: String, field: String, defaultField: String): File = {
//    val isDefaultField = defaultField == field
//    val realField = if (isDefaultField) "" else field
//    val pathName = (invFileBaseName+realField).stripSuffix("_") + "/"
//    val filePath = if (isDefaultField) pathName + term + ".inv" else pathName + term +"."+ field + ".inv"
//
//    try {
//      new File(filePath)
//    } catch {
//      case e: Exception => e.printStackTrace()
//      throw new IllegalArgumentException("Cannot find InvertedList at [%s]. Maybe field [%s] is not supported".format(filePath,field))
//      System.exit(1)
//      null
//    }
//  }

  /**
   * As homework 2 inverted lists directory and naming convention are a little bit different, a boolean is added
   * @param invFileBaseName Basename of the invertedFile
   * @param term The term to get the inverted list
   * @param field The field to get the inverted list
   * @param defaultField The default field name
   * @param isHw2 Indicating we are looking in homework 2. Default to be false
   * @return  The inverted file associated with the specific term and field name
   */
  def getInvertedFile(invFileBaseName: String, term: String, field: String, defaultField: String, isHw2:Boolean = false): File = {
    val isDefaultField = defaultField == field

    val pathName =
      if (isHw2)        {
        invFileBaseName+"/"+field + "/"
      } else{
        val realField = if (isDefaultField) "" else "_"+field
        invFileBaseName+realField + "/"
      }

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
