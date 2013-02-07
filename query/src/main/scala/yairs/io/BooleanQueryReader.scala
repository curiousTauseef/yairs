package yairs.io

import java.io.File
import io.Source
import yairs.model.BooleanQuery

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/6/13
 * Time: 9:38 PM
 */
class BooleanQueryReader(queryFile:File) extends QueryReader{
  def getQueries:List[BooleanQuery] = {
    val queries = Source.fromFile(queryFile).getLines.map(line => line.split(":")).map(fields => new BooleanQuery(fields(0),fields(1))).toList
    queries
  }
}
