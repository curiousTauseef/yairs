package yairs.model

/**
 * Created with IntelliJ IDEA.
 * User: Hector, Zhengzhong Liu
 * Date: 2/6/13
 * Time: 9:35 PM
 */
class BooleanQuery(id:String,query:String) extends Query{
  val DEFAULT_OPERATOR = "or"
  val queryId = id
  val queryString = query


  def prefixQueryParser(str:String,operator:String):List[String] = {
    if(operator == ""){
     if (str.startsWith("#")){
       if (str.startsWith("#OR")){
        List("or") :: prefixQueryParser(str.substring(2),"or")
       }else if(str.startsWith("#AND")){
        List("and") :: prefixQueryParser(str.substring(3),"and")
       }else{

       }
     }
    }else{

    }
  }

}
