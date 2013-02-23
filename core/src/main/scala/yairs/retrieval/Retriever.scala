package yairs.retrieval

import yairs.model.{Result, Query}

/**
 * Created with IntelliJ IDEA.
 * User: Hector
 * Date: 2/21/13
 * Time: 12:55 AM
 * To change this template use File | Settings | File Templates.
 */
trait Retriever {
  def evaluate(query: Query, runId:String): List[Result]
}
