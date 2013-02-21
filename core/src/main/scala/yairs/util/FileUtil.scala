package yairs.util

import java.io.File

/**
 * Created with IntelliJ IDEA.
 * User: Hector
 * Date: 2/21/13
 * Time: 1:26 AM
 * To change this template use File | Settings | File Templates.
 */
object FileUtil {
  def getInvertedFile(term: String): File = {
    new File("data/clueweb09_wikipedia_15p_invLists/" + term + ".inv")
  }
}
