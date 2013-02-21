package yairs.util

import java.io.{FileNotFoundException, FileOutputStream, FileInputStream, File}
import java.util.Properties
import org.eintr.loglady.Logging
import yairs.exceptions.ConfigurationException

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/20/13
 * Time: 5:53 PM
 */
class Configuration (val configFile:File) extends Logging {
    def this(fileName: String) {
      this(new File(fileName))
    }

    private val properties : Properties = new Properties()

    log.info("Loading configuration file "+configFile)
    properties.load(new FileInputStream(configFile))

    /**
     * Get a parameter by key, if not found then default value
     * @param key
     * @param defaultValue
     * @return
     */
    def getOrElse(key: String, defaultValue : String) : String = {
      properties.getProperty(key,defaultValue)
    }

    /**
     * Get a property value given a key
     * @param key
     * @return the property value as String
     */
    def get(key : String) : String = {
      var value = getOrElse(key, null)
      if (value == null) {
        throw new ConfigurationException(key + "not specified in "+configFile)
      }

      if (key.endsWith(".dir")) {
        if (!value.endsWith("/")){
          value += "/"
        }
      }

      value
    }
}
