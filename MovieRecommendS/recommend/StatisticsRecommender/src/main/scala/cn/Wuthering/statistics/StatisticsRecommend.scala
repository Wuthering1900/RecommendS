package cn.Wuthering.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *基于统计的推荐
 */

//mid       Int     电影的 ID
//name      String  电影的名称
//descri    String  电影的描述
//timelong  String  电影的时长
//shoot     String  电影拍摄时间
//issue     String  电影发布时间
//language  Array[String] 电影语言 每一项用“|”分割
//genres    Array[String] 电影所属类别 每一项用“|”分割
//director  Array[String] 电影的导演 每一项用“|”分割
//actors    Array[String] 电影的演员 每一项用“|”分割
case class Movie(mid:Int, name:String, descri:String, timelong:String, shoot:String,
                 issue:String, language:String, genres:String,
                 director:String, actors:String)

//uid Int 用户的 ID
//mid Int 电影的 ID
//score Double 电影的分值
//timestamp Long 评分的时间
case class Rating(uid:Int,mid:Int,score:Double,timestamp:Long)

case class MongoConfig(uri:String, db:String)


//定义一个基准推荐对象
case class Recommendation(mid:Int, score:Double)
//电影类别top10
case class genresRecommendation(genres:String,recs:Seq[Recommendation])

object StatisticsRecommend {

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表名
  val RATE_MORE_MOVIES = "RateMoreMovies"  //评分最多电影
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreMovies"       //近期热门
  val AVG_RATE_MOVIES = "AvgMovies"        //优质电影(平均评分最高)
  val GENRES_RATE_MORE_MOVIES = "GenresTopMovies"        //类别，电影

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender")

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("Statistics")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))


    //加载数据
    val MovieDf = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
    val ratingDf = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]

    //创建临时表
    ratingDf.createOrReplaceTempView("ratings")

    //历史热门统计，评分数据最多
    val rateMoreMoviesDF = spark.sql("select mid,count(mid) as count from ratings group by mid")
    storeMongodbDf(rateMoreMoviesDF,RATE_MORE_MOVIES)
    //近期热门统计，按照‘yyyyMM’选取最近评分，统计个数
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changData",(x:Int)=>simpleDateFormat.format(new Date(x*1000L)).toInt)
    //优质电影统计，平均评分

    //各类别热门统计

    spark.stop()

  }

  def storeMongodbDf(df : DataFrame,collection_name:String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .option("uri",mongoConfig.uri)
      .option("collection",collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
