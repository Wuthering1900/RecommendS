package cn.Wuthering.streaming


import com.mongodb.client.MongoClients
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

//定义连接助手对象，序列化
object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("localhost",6379)
  lazy val mongoClient = MongoClients.create("mongodb://localhost:27017/recommender")
}



case class MongoConfig(uri:String, db:String)
//定义一个基准推荐对象
case class Recommendation(mid:Int, score:Double)
//定义基于预测评分的用户推荐列表
case class UserRecs(uid:Int,recs:Seq[Recommendation])
//定义基于LFM电影特征向量的电影相似度列表
case class MovieRecs(mid:Int,recs:Seq[Recommendation])

case class MoviesRating(uid:Int,mid:Int,score:Double,timestamp:Long)

object StreamingRecommended {

  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_MOVIE_COLLECTION = "MovieRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_STREAM_MOVIE_COLLECTION = "StreamRecs"




  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka,topic"->"recommender"
    )


    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommended")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //拿到streaming context
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc,Seconds(2))

    import spark.implicits._
    
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))


    val simMovie = spark.read
        .option("uri",mongoConfig.uri)
        .option("collection",MONGODB_MOVIE_COLLECTION)
        .format("com.mongodb.spark.sql")
        .load()
        .as[MovieRecs]
      //map方便查询
    val simMovieMatrix = simMovie.rdd
        .map { moverRecs => {
          (moverRecs.mid, moverRecs.recs.map(x => (x.mid, x.score)).toMap)
          }
        }.collectAsMap()
    val simMovieMatrixBroadcast = sc.broadcast(simMovieMatrix)

    //kafka配置
    val kafkaParam = Map(
      "bootstrap"->"localhost",
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id"->"recommender",
      "auto.offset.reset"->"latest"
    )

    //通过kafka创建一个DStream
    val kafkaStream = KafkaUtils.createDirectStream[String,String](
      ssc: StreamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")),kafkaParam),
    )

    //把原始数据转化为评分流  uid|mid|score|timestamp
    val ratingStream = kafkaStream.map{
      msg=>
        val attr = msg.value().split("\\|")
        (attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }

    //继续做流式处理，核心实时算法部分
    ratingStream.foreachRDD{
      rdds=>rdds.foreach{
        case(uid,mid,score,timestamp) => {
          println(("rating data coming! >>>>>>>>>>>>>>"))

          //1.从redis里获取当前用户的K次评分，保存成Array（mid,score）

          val userRecentRatings = getUserRecentRatings(ConnHelper.jedis,uid,MAX_USER_RATING_NUM)
          //2.从相似度矩阵中取出当前电影最相似的n个电影作为备选列表，Array[mid]
          val candidates = getTopSimMovies(simMovieMatrixBroadcast.value,spark,mid,uid,MAX_SIM_MOVIES_NUM)
          //3.对每个被选电影，计算推荐优先级，得到当前用户的实施推荐列表，Array(mid,score)
          val recommendMovies = computeMoviesScore(userRecentRatings, candidates, simMovieMatrixBroadcast.value)
          //4.把推荐数据保存到mongodb
          saveDateToMongoDB(uid,recommendMovies)

        }
      }
    }

    ssc.start()

    println(">>>>>>>>>>>>>>>>>>>>>>>>>> streaming started!!!")

    ssc.awaitTermination()

    spark.stop()

  }

  import scala.collection.JavaConverters
  def getUserRecentRatings(jedis: Jedis, uid: Int, num: Int):Array[(Int,Double)] = {
    JavaConverters.asScalaBuffer(jedis.lrange("uid:"+uid,0,num-1))
      .map{
        item=>
          val attr = item.split("\\:")
          (attr(0).toInt,attr(1).toDouble)
      }
      .toArray

  }

  /**
   * 获取和当前电影相似的num个备选电影
   * @param simMovie  相似度矩阵
   * @param mid
   * @param uid
   * @param NUM
   * @return 备选电影
   */

  def getTopSimMovies(simMovie: collection.Map[Int, Map[Int, Double]], spark: SparkSession,mid: Int, uid: Int, NUM: Int)
                     (implicit mongoConfig: MongoConfig) : Array[Int]={
    val allSimMovie = simMovie(mid).toArray
    import spark.implicits._
    val ratingExit = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MoviesRating]
      .rdd
      .filter{
        rating => rating.uid==uid
      }
      .map( rating => rating.mid)
      .collect()

    allSimMovie.filter(x=>ratingExit.contains(x._1))
          .sortWith(_._2>_._2)
          .take(NUM)
          .map(x=>x._1)

//    val collection = ConnHelper.mongoClient.getDatabase(mongoConfig.db).getCollection(MONGODB_RATING_COLLECTION)
//    val ratingExit =collection.find( new BsonDocument("uid",new BsonInt64(uid)))
//      .map{
//        item=>item.get(mid).toString.toInt
//      }
//    Array(ratingExit)
//    allSimMovie.filter(x=>Array(ratingExit).contains(x._1))
//      .sortWith(_._2>_._2)
//      .take(NUM)
//      .map(x=>x._1)
  }
  //获取两个电影的相似度
  def getMovieSimScore(candidate: Int, userRecentRatingMovie: Int, simMovie: collection.Map[Int, Map[Int, Double]]):Double = {
    simMovie.get(candidate)match {
      case Some(sims)=>sims.get(userRecentRatingMovie)match {
        case Some(score) =>score
        case None =>0.0
      }
      case None =>0.0
    }
  }

  def computeMoviesScore(userRecentRatings:Array[(Int,Double)], candidates: Array[Int], simMovie: collection.Map[Int, collection.immutable.Map[Int, Double]]) :Array[(Int,Double)] ={
    val scores = collection.mutable.ArrayBuffer[(Int,Double)]()
    val increMap = collection.mutable.HashMap[Int,Int]()
    val decreMap = collection.mutable.HashMap[Int,Int]()

    for(candidate <- candidates;userRecentRating <- userRecentRatings){
      //获取备选电影和最近评分电影的相似度
      val simScore = getMovieSimScore(candidate,userRecentRating._1,simMovie)
      if(simScore > 0.7) {
        scores += (( candidate, simScore * userRecentRating._2))
        if(userRecentRating._2 > 3){
          increMap(candidate) = increMap.getOrElse(candidate,0)+1
        }
        else{
          decreMap(candidate) = decreMap.getOrElse(candidate,0)+1
        }
      }
    }
    //根据被选电影的id做groupby,根据公式做最后的推荐评分
    scores.groupBy(_._1).map{
      case(mid,scoreList)=>{
        (mid,scoreList.map(_._2).sum/scoreList.length +
          log( increMap.getOrElse(mid,1)) -
          log( decreMap.getOrElse(mid,1)))
      }
    }.toArray
  }

  def log(i: Int): Double ={
    val N = 10
    math.log(i)/math.log(10)
  }

  def saveDateToMongoDB(uid: Int, recommendMovies: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
    //val collection =   ConnHelper.mongoClient.getDatabase(mongoConfig.db).getCollection(MONGODB_STREAM_MOVIE_COLLECTION)

    //如果已有uid 删除
   
    //将数据存入


  }
}
