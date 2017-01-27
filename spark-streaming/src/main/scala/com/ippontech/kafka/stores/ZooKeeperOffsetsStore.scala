package com.ippontech.kafka.stores

import com.ippontech.kafka.util.Stopwatch
import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.common.TopicAndPartition
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

class ZooKeeperOffsetsStore(zkHosts: String, zkPath: String) extends OffsetsStore with LazyLogging {

  private val zkClient = new ZkClient(zkHosts, 10000, 10000, ZKStringSerializer)

  // Read the previously saved offsets from Zookeeper
  override def readOffsets(topics: Set[String]): Option[Map[TopicAndPartition, Long]] = {

    logger.info("Reading offsets from ZooKeeper")
    val stopwatch = new Stopwatch()

    val offsets = topics.toList.flatMap(topic => readOffsets(topic))

    logger.info("Done reading offsets from ZooKeeper. Took " + stopwatch)

    offsets match {
      case Nil => None
      case _ => Some(offsets.toMap)
    }
  }

  private def readOffsets(topic: String): Seq[(TopicAndPartition, Long)] = {
    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, s"${zkPath}/${topic}")

    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        logger.debug(s"Read offset ranges: ${zkPath}/${topic} -> ${offsetsRangesStr}")

        val offsets: Array[(TopicAndPartition, Long)] = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt), offsetStr.toLong) }

        logger.info(s"Done reading offsets for topic ${topic} from ZooKeeper")

        offsets
      case None =>
        logger.info(s"No offsets found for topic ${topic} in ZooKeeper")
        Seq()
    }
  }

  // Save the offsets back to ZooKeeper
  //
  // IMPORTANT: We're not saving the offset immediately but instead save the offset from the previous batch. This is
  // because the extraction of the offsets has to be done at the beginning of the stream processing, before the real
  // logic is applied. Instead, we want to save the offsets once we have successfully processed a batch, hence the
  // workaround.
  override def saveOffsets(topics: Set[String], rdd: RDD[_]): Unit = {

    logger.info("Saving offsets to ZooKeeper")
    val stopwatch = new Stopwatch()

    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetsRanges.foreach(offsetRange => logger.debug(s"Using ${offsetRange}"))

    offsetsRanges.groupBy(offsetRange => offsetRange.topicAndPartition().topic)
      .foreach{ case (topic, topicOffsetRanges) => saveOffsets(topic, topicOffsetRanges) }

    logger.info("Done updating offsets in ZooKeeper. Took " + stopwatch)

  }

  private def saveOffsets(topic: String, offsetRanges: Seq[OffsetRange]): Unit = {

    logger.info(s"Saving offsets for topic ${topic} to ZooKeeper")

    val offsetsRangesStr = offsetRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    logger.debug(s"Writing offsets for topic to ZooKeeper: ${zkPath}/${topic} -> ${offsetsRangesStr}")
    ZkUtils.updatePersistentPath(zkClient, s"${zkPath}/${topic}", offsetsRangesStr)

    logger.info(s"Done updating offsets for topic ${topic} in ZooKeeper")

  }

}
