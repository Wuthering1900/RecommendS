package cn.Wuthering.offline

import breeze.numerics.sqrt
import cn.Wuthering.offline.OfflineRecommend.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession



object ALSTrainer {
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender")
    //创建一个sparkSession
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("offline")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    // 加载数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MoviesRating]
      .rdd
      .map( rating => Rating( rating.uid, rating.mid, rating.score ) )    // 转化成rdd，并且去掉时间戳
      .cache()

    //随即切分 训练集和测试集
    val splits = ratingRDD.randomSplit(Array(0.8,0.2))
    val trainRDD = splits(0)
    val testRDD = splits(1)

    //模型参数调优，返回合适参数
    adjustALSParam(trainRDD,testRDD)

    spark.stop()
  }

  def adjustALSParam(trainRDD: RDD[Rating], testRDD: RDD[Rating]): Unit ={
    val result = for(rank <- Array(50,100,200);lambda <- Array(0.01,0.1,0.1))
      yield {
        val model = ALS.train(trainRDD, rank, 5, lambda)
        val rmse = getRMSE(model,testRDD)
        (rank,lambda,rmse)
      }
    //打印最优参数
    println(result.minBy(_._3))

  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]):Double={

    val userproduct = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userproduct)

    val observed = data.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))

    sqrt(
        observed.join(predict).map{
        case ((uid,mid),(actual,predict)) =>
          val err = actual-predict
          err*err
      }.mean()
    )

  }


}
