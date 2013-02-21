package yairs.io

import java.io.File
import io.Source
import yairs.model.BooleanQuery
import org.eintr.loglady.Logging

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/6/13
 * Time: 9:38 PM
 */
class BooleanQueryReader extends QueryReader{
  @Override
  def getQueries(queryFile:File):List[BooleanQuery] = Source.fromFile(queryFile).getLines().map(line => line.split(":")).map(fields => new BooleanQuery(fields(0),fields(1))).toList


  def getQuery(qid:String,queryString:String):BooleanQuery = new BooleanQuery(qid,queryString)
}

object BooleanQueryReader extends Logging{
  def main(args : Array[String]) {
    log.debug("Try some simple queries")

    val qr = new BooleanQueryReader()

    //testQuery(qr)
    testQueries(qr)
    log.debug("Done.")
  }

  def testQuery(qr:BooleanQueryReader){
    val query0 = qr.getQuery("1","#OR (#AND (viva la vida) coldplay)")
    query0.dump()

    val  query1 = qr.getQuery("2","(#AND (viva la vida) coldplay)")
    query1.dump()

    val query2 = qr.getQuery("3","#AND (viva la vida) coldplay")
    query2.dump()

    val query3 = qr.getQuery("4","(viva la vida) coldplay")
    query3.dump()
  }

  def testQueries(qr:BooleanQueryReader){
    qr.getQueries(new File("data/queries.txt")).foreach(q => q.dump())
  }
}
