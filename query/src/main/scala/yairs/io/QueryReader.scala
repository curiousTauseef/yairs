package yairs.io

import yairs.model.Query
import java.io.File

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/6/13
 * Time: 9:29 PM
 */
trait QueryReader {
     def getQueries(queryFile: File):List[Query]
}
