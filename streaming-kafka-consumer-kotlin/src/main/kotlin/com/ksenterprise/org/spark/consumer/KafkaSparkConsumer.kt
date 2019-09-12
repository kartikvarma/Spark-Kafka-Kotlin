package com.ksenterprise.org.spark.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka010.*
import java.util.concurrent.atomic.AtomicReference


fun main() {

    val kafkaParams = HashMap<String, Any>()
    kafkaParams["bootstrap.servers"] = "localhost:9092,anotherhost:9092"
    kafkaParams["key.deserializer"] = StringDeserializer::class.java
    kafkaParams["value.deserializer"] = StringDeserializer::class.java
    kafkaParams["group.id"] = "NumbersGroup"
    kafkaParams["auto.offset.reset"] = "latest"
    kafkaParams["enable.auto.commit"] = false

    val sparkConf = SparkConf().setAppName("NumberStream").setMaster("local[*]")
    sparkConf.set("spark.streaming.backpressure.enable", "true")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "2000")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KyroSerializer")

    sparkConf.registerKryoClasses(arrayOf(ConsumerRecord::class.java))

    JavaStreamingContext(sparkConf, Durations.seconds(2)).use { jsc ->

        val fromOffsets = HashMap<TopicPartition, Long>()
        fromOffsets[TopicPartition("numbers",0)] = 0L

        val offsetRanges = AtomicReference<Array<OffsetRange>>()

        val stream = KafkaUtils.createDirectStream(
            jsc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Assign<String, String>(fromOffsets.keys, kafkaParams, fromOffsets)
        )

        stream
            .transform { rdd ->
                val hasOffsetRanges = rdd?.rdd() as HasOffsetRanges
                val offSets = hasOffsetRanges.offsetRanges()
                offsetRanges.set(offSets)
                rdd
            }.foreachRDD { rdd -> rdd?.collect()?.forEach { println("Topic ${it.topic()} produced message : ${it.value()}") } }

        jsc.start()
        jsc.awaitTermination()
    }
}





