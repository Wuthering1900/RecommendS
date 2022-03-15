package cn.Wuthering.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

/**
 * 基于隐语义模型的推荐
 * 隐语义模型的协同过滤  根据评分数据
 */


//uid Int 用户的 ID
//mid Int 电影的 ID
//score Double 电影的分值
//timestamp Long 评分的时间
case class MoviesRating(uid:Int,mid:Int,score:Double,timestamp:Long)

case class MongoConfig(uri:String, db:String)

//定义一个基准推荐对象
case class Recommendation(mid:Int, score:Double)

//定义基于预测评分的用户推荐列表
case class UserRecs(uid:Int,recs:Seq[Recommendation])

//定义基于LFM电影特征向量的电影相似度列表
case class MovieRecs(mid:Int,recs:Seq[Recommendation])

object OfflineRecommend {

  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val USER_MAX_RECOMMENDATION = 10


  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender")

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("offline")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    // 加载评分数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MoviesRating]
      .rdd
      .map( rating => Rating( rating.uid, rating.mid, rating.score ) )    // 转化成rdd，并且去掉时间戳
      .cache()

    // 从rating数据中提取所有的uid和mid，并去重
    val userRDD = ratingRDD.map(_.user).distinct()
    val movieRDD = ratingRDD.map(_.product).distinct()

    // 训练隐语义模型
    val (rank, iterations, lambda) = (200, 5, 0.1)
    val model = ALS.train(ratingRDD, rank, iterations, lambda)

    //基于用户和电影的隐特质，预测电影评分，得到用户推荐列表
    val userMovie = userRDD.cartesian(movieRDD)
    val preRDD: RDD[Rating] = model.predict(userMovie)
    val userRec = preRDD
        .filter(_.rating>0)
        .map(ratings =>(ratings.user,(ratings.product,ratings.rating) ))
        .groupByKey()
        .map{
          case (uid,recs) => UserRecs(uid,recs.toList.sortWith(_._2>_._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1,x._2)))
        }
        .toDF()
    userRec.write
        .option("uri",mongoConfig.uri)
        .option("collection",USER_RECS)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()
    //基于电影的隐特质，计算相似度矩阵，得到电影相似度列表

    val movieFeatures = model.productFeatures.map{
      case(mid,features) => (mid,new DoubleMatrix(features))
    }
    //对所有电影两两计算它们的相似度
    val movieRecs: DataFrame = movieFeatures.cartesian(movieFeatures)
      .filter {
        case (a, b) => a._1 != b._1
      }
      .map {
        case (a, b) => {
          val simScore = consinSim(a._2, b._2)
          (a._1, (b._1, simScore))
        }
      }
      .filter(_._2._2 > 0.6) //过滤出相似度大于0.6的
      .groupByKey()
      .map {
        case (mid, items) => MovieRecs(mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()
    movieRecs.write
        .option("uri",mongoConfig.uri)
        .option("collection",MOVIE_RECS)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

    spark.stop()
  }

  def consinSim(matrix: DoubleMatrix, matrix1: DoubleMatrix):Double ={
    matrix.dot(matrix1)/(matrix.norm2()*matrix1.norm2())
  }


}
