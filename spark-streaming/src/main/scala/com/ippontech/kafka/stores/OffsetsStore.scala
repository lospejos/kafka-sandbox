package com.ippontech.kafka.stores

import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD

trait OffsetsStore {

  def readOffsets(topics: Set[String]): Option[Map[TopicAndPartition, Long]]

  def saveOffsets(topics: Set[String], rdd: RDD[_]): Unit

}
