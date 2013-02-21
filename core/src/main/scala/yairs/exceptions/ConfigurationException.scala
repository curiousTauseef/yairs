package yairs.exceptions

import java.lang.String

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/20/13
 * Time: 5:55 PM
 */
class ConfigurationException(message: String, cause: Throwable) extends Exception {
  if (cause != null)
    initCause(cause)

  def this(message: String) = this(message, null)
}
