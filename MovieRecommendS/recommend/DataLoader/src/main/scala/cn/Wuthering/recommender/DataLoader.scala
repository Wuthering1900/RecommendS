package cn.Wuthering.recommender

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.http.HttpHost
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.RestClient
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Indexes

/**
 * 冷启动
 * 单例
 * 数据加载
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

//uid Int 用户的 ID
//mid Int 电影的 ID
//tag String 电影的标签
//timestamp Long 标签的时间
case class Tags(uid:Int,mid:Int,tag:String,timestamp:Long)

case class MongoConfig(uri:String, db:String)
case class ESConfig(httpHosts:String, transportHosts:String, index:String, clustername:String)


object DataLoader {
  //
  val MOVIE_DATA_PATH = "D:\\创\\javaidea\\MovieRecommendS\\recommend\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\创\\javaidea\\MovieRecommendS\\recommend\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\创\\javaidea\\MovieRecommendS\\recommend\\DataLoader\\src\\main\\resources\\tags.csv"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {
    //常用配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "localhost:9200",
      "es.transportHosts" -> "localhost:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )
    //创建一个sparkconf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    //创建一个sparkSession
    val spark = SparkSession.builder().config(sparkConf)
//      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/recommender")
//      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/recommender")
      .getOrCreate()



    //加载数据
    import spark.implicits._
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF = movieRDD.map(
      item=>{
        val attr = item.split("\\^")
        Movie(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
      }
    ).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(
      item => {
        val attr = item.split(",")
        Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
      }).toDF()

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF = tagRDD.map(
      item=>{
        val attr =item.split(',')
        Tags(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
      }
    ).toDF()


    //数据保存mongodb
    //MongoSpark.save(movieDF.write.mode(SaveMode.Overwrite))

    //    MongoSpark.save(movieDF.write.option("collection", MONGODB_MOVIE_COLLECTION).mode("overwrite"))
    //    MongoSpark.save(ratingDF.write.option("collection", MONGODB_RATING_COLLECTION).mode("overwrite"))
    //    MongoSpark.save(tagDF.write.option("collection", MONGODB_TAG_COLLECTION).mode("overwrite"))

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    storeDataInMongodb(movieDF,ratingDF,tagDF)

    //数据预处理 将电影信息和标签整合在一起
    import org.apache.spark.sql.functions._
    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|",collect_set($"tag")).as("tags"))
      .select("mid","tags")

    //newtag和movie合并在一起 左外连接
    val movieWithTag = movieDF.join(newTag,Seq("mid"),"left")

    //数据保存es
    implicit val esConfig = ESConfig( config("es.httpHosts"),
                                      config("es.transportHosts"),
                                      config("es.index"),
                                      config("es.cluster.name"))
    storeDataInES(movieWithTag)

    spark.close()

  }

  def storeDataInES(movieDF:DataFrame)(implicit esConfig: ESConfig): Unit ={
    val hostname = esConfig.httpHosts.split(":")(0)
    val port = esConfig.httpHosts.split(":")(1).toInt

    val esClient = new RestHighLevelClient(
      RestClient.builder(new HttpHost(hostname,port,"http"))
    )

    //保存
    movieDF.write
        .option("es.node",esConfig.httpHosts)
        .option("es.http.timeout","100m")
        .option("es.mapping.id","mid")
        .mode("overwrite")
        .format("org.elasticsearch.spark.sql")
        .save(esConfig.index+"/"+ES_MOVIE_INDEX)

    //关闭
    esClient.close()

  }


  /**
   *
   */
  def storeDataInMongodb(movieDF:DataFrame, ratingDF:DataFrame, tagDF:DataFrame)(implicit mongoConfig: MongoConfig): Unit ={

    val mongoClient = MongoClients.create(mongoConfig.uri)
    val database = mongoClient.getDatabase(mongoConfig.db)

    //删除原有集合（覆盖不需要删除）
    database.getCollection(MONGODB_MOVIE_COLLECTION).drop()
    database.getCollection(MONGODB_RATING_COLLECTION).drop()
    database.getCollection(MONGODB_TAG_COLLECTION).drop()
    //存储数据

    movieDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //创建索引
    database.getCollection(MONGODB_MOVIE_COLLECTION).createIndex(Indexes.ascending("mid"))
    database.getCollection(MONGODB_RATING_COLLECTION).createIndex(Indexes.ascending("uid"))
    database.getCollection(MONGODB_RATING_COLLECTION).createIndex(Indexes.ascending("mid"))
    database.getCollection(MONGODB_TAG_COLLECTION).createIndex(Indexes.ascending("uid"))
    database.getCollection(MONGODB_TAG_COLLECTION).createIndex(Indexes.ascending("mid"))
    mongoClient.close()
  }
}
