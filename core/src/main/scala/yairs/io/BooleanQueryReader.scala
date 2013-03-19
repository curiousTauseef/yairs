package yairs.io

import java.io.File
import io.Source
import yairs.model.{Query, BooleanQuery}
import org.eintr.loglady.Logging
import yairs.util.Configuration

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/6/13
 * Time: 9:38 PM
 */
class BooleanQueryReader(config:Configuration) extends QueryReader{
  @Override
  def getQueries(queryFile:File):List[Query] = Source.fromFile(queryFile).getLines().map(line => line.split(":")).map(fields => new BooleanQuery(fields(0),fields(1),config)).toList

  def getQuery(qid:String,queryString:String):Query = new BooleanQuery(qid,queryString,config)
}

object BooleanQueryReader extends Logging{
  def main(args : Array[String]) {
    if (args.length == 0) {
      log.error("Please supply the configuration file path as command line parameter")
      System.exit(1)
    }
    val configurationFileName = args(0)
    val config = new Configuration(configurationFileName)

    log.debug("Try some simple queries")

    val qr = new BooleanQueryReader(config)

    testQuery(qr)
    //testQueries(qr)
    log.debug("Done.")
  }

  private def testQuery(qr:BooleanQueryReader){
    val query0 = qr.getQuery("1","#OR (#AND (viva la vida) coldplay)")
    query0.dump()

    val  query1 = qr.getQuery("2","(#AND (viva la vida) coldplay)")
    query1.dump()

    val query2 = qr.getQuery("3","#AND (viva la vida) coldplay")
    query2.dump()

    val query3 = qr.getQuery("4","(viva la vida) coldplay")
    query3.dump()

    val query4 = qr.getQuery("5","(#NEAR/2(viva la) vida) coldplay")
    query4.dump()

    val query5 = qr.getQuery("6","((#NEAR/2(viva la) vida)) coldplay")
    query5.dump()

    val query6 = qr.getQuery("7","#AND (#NEAR/1 (arizona states) obama)")
    query6.dump()

    val query7 = qr.getQuery("8","#NEAR/1 arizona states")
    query7.dump()

    val query8 = qr.getQuery("9","arizona+title states+title")
    query8.dump()

    val query9 = qr.getQuery("10","#NEAR/4 (poker tournaments)")
    query9.dump()

    val query10 = qr.getQuery("11","#OR (#NEAR/2 (alexian brothers) hospital)")
    query10.dump()

    val query11 = qr.getQuery("12","er #NEAR/2 (tv show)")
    query11.dump()

    val query12 = qr.getQuery("13","#WEIGHT(0.5 texas 0.4 hotel 0.1 convention )")
    query12.dump()

    val query13 = qr.getQuery("14", "#UW/2( border texas )")
    query13.dump()

    val query14 = qr.getQuery("15", "#AND( #NEAR/2( south africa ) fish )")
    query14.dump()

    val query15 = qr.getQuery("16", "#UW/2( border texas )")
  }

  def testQueries(qr:BooleanQueryReader){
    qr.getQueries(new File("data/queries.txt")).foreach(q => q.dump())
  }
}
