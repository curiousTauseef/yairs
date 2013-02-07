package yairs.io

import yairs.model.Query

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/6/13
 * Time: 9:29 PM
 */
trait QueryReader {
     def getQueries:List[Query]
}
