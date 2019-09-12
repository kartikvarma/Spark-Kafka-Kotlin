package com.ksenterprise.org.spark.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;


@Slf4j
public class KafkaConsumer {

    public static void main(String[] args) {

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");

        SparkConf sparkConf = new SparkConf().setAppName("NumberStream").setMaster("localhost:7077");
        sparkConf.set("spark.streaming.backpressure.enable", "true");
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "2000");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KyroSerializer");

        sparkConf.registerKryoClasses((Class<?>[]) Collections.singletonList(ConsumerRecord.class).toArray());


        try (JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(2))) {

            Map<TopicPartition, Long> offSetMap = new HashMap<>();
            offSetMap.put(new TopicPartition("numbers", 0), 0L);

            final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),ConsumerStrategies.Assign(offSetMap.keySet(), kafkaParams, offSetMap));
            // Transform
            stream.transform((Function<JavaRDD<ConsumerRecord<String, String>>, JavaRDD<ConsumerRecord<String, String>>>) rdd -> {
                OffsetRange[] offSets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offSets);
                return rdd;
            }).foreachRDD((VoidFunction<JavaRDD<ConsumerRecord<String, String>>>) consumerRecordJavaRDD -> {
                List<ConsumerRecord<String, String>> collect = consumerRecordJavaRDD.collect();
                collect.forEach(consumerRecord -> System.out.println(String.format("Topic %s produced message : %s", consumerRecord.topic(), consumerRecord.value())));
            });
            jsc.start();
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            log.error(e.getMessage(),e);
        }
    }
}
