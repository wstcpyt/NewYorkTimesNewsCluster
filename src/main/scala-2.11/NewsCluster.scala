import com.firebase.client.Firebase
import org.apache.spark.{SparkContext, SparkConf}
import org.json4s._
import org.json4s.jackson.Serialization.{read,write}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg._

import scala.collection.immutable.HashMap

object NewsCluster {
  def sumArray (m: Array[Double], n: Array[Double]): Array[Double] = {
    for (i <- m.indices) {m(i) += n(i)}
    m
  }

  def divArray (m: Array[Double], divisor: Double) : Array[Double] = {
    for (i <- m.indices) {m(i) /= divisor}
    m
  }

  def wordToVector (w:String, m: Word2VecModel): Vector = {
    try {
      m.transform(w)
    } catch {
      case e: Exception =>  Vectors.zeros(100)
    }
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val news_rdd = sc.textFile("/Users/yutongpang/SparkProject/newscluster/items").map(s => s.replace(",", ""))
    println("-------------Attach debugger now!--------------")
    Thread.sleep(2000)

    val news_json = news_rdd.map(record => {
      implicit val formats = DefaultFormats
      read[NewsArticle](record)
    })
    val news_titles = news_json.map(_.title.split(" ").toSeq)
    val news_title_words = news_titles.flatMap(x => x).map(x => Seq(x))
    val all_input = news_title_words
    val word2vec = new Word2Vec()
    val model = word2vec.fit(all_input)
    val title_vectors = news_titles.map(x => new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector]).cache()
    val title_pairs = news_titles.map(x => (x,new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector]))
    val numClusters = 200
    val numIterations = 25
    val clusters = KMeans.train(title_vectors, numClusters, numIterations)
    val wssse = clusters.computeCost(title_vectors)
    val article_membership = title_pairs.mapValues(x => clusters.predict(x))
    println("====save to ====")
    val rootRef = new Firebase("https://clustercomponent.firebaseio.com/nytimescluster")
    0 until 200 foreach{ i =>
      val sample_members = article_membership.filter(x => x._2 == i).take(5)
      sample_members.zipWithIndex.foreach{x =>
        println(x._1._1.mkString(","))
        val newref = rootRef.push()
        newref.child("title").setValue(x._1._1.mkString(","))
        newref.child("cluster").setValue(i.toString)
      }
      println("")
    }
    println("stop")
  }
}
