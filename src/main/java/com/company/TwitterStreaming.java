package com.company;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

public class TwitterStreaming {

  public static void main(String[] args) {
    System.setProperty("twitter4j.oauth.consumerKey", "Your Consumer Key");
    System.setProperty("twitter4j.oauth.consumerSecret", "Your Consumer Secret");
    System.setProperty("twitter4j.oauth.accessToken", "Your Access Token");
    System.setProperty("twitter4j.oauth.accessTokenSecret", "Your Access Token Secret");

    SparkConf config = new SparkConf()
            .setAppName("TwitterStreaming")
            .setMaster("local[8]");

    JavaStreamingContext jsc = new JavaStreamingContext(config, Durations.seconds(30));

    JavaDStream<Status> tweets =  TwitterUtils.createStream(jsc);

    JavaDStream<String> statuses = tweets.map(
                    new Function<Status, String>() {
                      public String call(Status status) { return status.getText(); }
                    }
            );

    statuses.print();

    jsc.start();
    jsc.awaitTermination();

  }
}