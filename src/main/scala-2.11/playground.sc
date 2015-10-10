import com.google.gson.Gson
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read,write}

import scala.io.Source
var gson = new Gson()
implicit val formats = DefaultFormats
for (line <- Source.fromFile("/Users/yutongpang/SparkProject/newscluster/items").getLines())
  println(read[NewsArticle](line).title.split(" ").toSeq)