package cn.Wuthering.recommender

import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}


object EsClient {
  def main(args: Array[String]): Unit = {

    val esClient = new RestHighLevelClient(
      RestClient.builder(new HttpHost("localhost",9200,"http"))
    )
    val request = new GetIndexRequest("shopping")

    val request2 = new BulkRequest()
    val response = esClient.indices().get(request, RequestOptions.DEFAULT);
    System.out.println(response.getAliases())
    System.out.println(response.getMappings())

    esClient.close()
  }

}
