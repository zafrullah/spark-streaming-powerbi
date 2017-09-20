/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.spark.streaming.modules.workloads



import java.nio.charset.Charset
import java.util.Date

import com.microsoft.spark.streaming.modules.common.{EventPayload}
import org.apache.http.HttpResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.EntityBuilder
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.spark._
import org.apache.spark.streaming.eventhubs.EventHubsUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.util.Random


object EventhubsDStream {

  def pushdata(maxval:String)={

    //Get url from power BI streaming data set
    val url = "https://api.powerbi.com/beta/subscription-id/datasets/key"
    val now = new Date()
    val utcString = new DateTime(now).withZone(DateTimeZone.UTC).toString()

    val row:String = "[{\"EventID\" :\"AAAAA555555\",\"EventVal\" :"+ maxval +",\"EventTime\" :\""+ utcString +"\"}]"


    val CONNECTION_TIMEOUT_MS = 20000; // Timeout in millis (20 sec).

    val requestConfig = RequestConfig.custom()
      .setConnectionRequestTimeout(CONNECTION_TIMEOUT_MS)
      .setConnectTimeout(CONNECTION_TIMEOUT_MS)
      .setSocketTimeout(CONNECTION_TIMEOUT_MS)
      .build();

    HttpClientBuilder.create().build();

    val client: CloseableHttpClient = HttpClientBuilder.create().build();

    val post = new HttpPost(url);

    //Set config to post
    post.setConfig(requestConfig)

    post.setEntity(EntityBuilder.create.setText(row).build())

    val response: HttpResponse = client.execute(post)

  }


  def main(args: Array[String]): Unit = {

    //Initialize PowerBI Authentication


    val eventHubsParameters = Map[String, String](
      "eventhubs.namespace" -> "enter event hub namespace",
      "eventhubs.name" -> "enter event hub name",
      "eventhubs.policyname" -> "enter policy name",
      "eventhubs.policykey" -> "enter key=",
      "eventhubs.consumergroup" -> "enter consumer group",
      "eventhubs.partition.count" -> "enter parition count"
    )

    val progressDir: String = "/EventCheckpoint"
    val sparkConfiguration = new SparkConf().setAppName(this.getClass.getSimpleName)


    val sparkContext = new SparkContext(sparkConfiguration)
    val streamingContext = new StreamingContext(sparkContext, Seconds(5))
    streamingContext.checkpoint("/EventHubStreamingPOC")

    val inputDirectStream = EventHubsUtils.createDirectStreams(
      streamingContext,
      "enter event hub namespace",
      progressDir,
      Map("enter event hub name" -> eventHubsParameters))

    val eventStream = inputDirectStream
      .map(messageStr => {
          implicit val formats = DefaultFormats;
          val event = parse(new String(messageStr.getBytes, Charset.defaultCharset())).extract[EventPayload]
          (event.payloadID,event.payloadVal)
      })

    val randomGenerator: Random = new Random()

    eventStream.foreachRDD(rdd => {
      val randVal = randomGenerator.nextDouble()*100
      val maxVal = rdd.map(_._2).max() + randVal
      pushdata(maxVal.toString)
      println(s"Max $maxVal")

    })
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}



