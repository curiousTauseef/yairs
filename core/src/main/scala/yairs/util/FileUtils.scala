package yairs.util

import java.io.File
import yairs.model.QueryField

/**
 * Created with IntelliJ IDEA.
 * User: Hector
 * Date: 2/21/13
 * Time: 1:26 AM
 * To change this template use File | Settings | File Templates.
 */
object FileUtils {
  def getInvertedFile(invFileBaseName:String, term: String, field: QueryField.Value): File = {
    if (field ==QueryField.BODY)
      new File(invFileBaseName + "/" + term + ".inv")
    else if (field == QueryField.TITLE){
      val invPath = invFileBaseName + "_title/" + term + ".title.inv"
      println(invPath)
      new File(invPath)
    }
    else throw new IllegalArgumentException("Currently only support BODY and TITLE fields.")
  }
}
