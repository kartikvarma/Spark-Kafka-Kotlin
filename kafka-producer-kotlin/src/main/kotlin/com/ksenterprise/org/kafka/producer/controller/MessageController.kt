package com.ksenterprise.org.kafka.producer.controller

import com.fasterxml.jackson.databind.ObjectMapper
import com.ksenterprise.org.kafka.producer.domain.Message
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.*

@RestController
class MessageController(private val kafkaTemplate: KafkaTemplate<Int, String>) {

    @GetMapping("/{topic}/publishMessages")
    fun publishMessagesInBatch(@PathVariable("topic", required = true )topic: String, @RequestParam(value = "batchSize", required = true) batchSize: Int) {
        for (x in 0..batchSize) {
            this.kafkaTemplate.send(topic, ObjectMapper().writeValueAsString(Message("This is $x message", x)))
        }
    }

    @PostMapping("/listener/{offset}")
    fun listenToMessages(@PathVariable(value="offset", required = true)offset: Int, @RequestBody messages: List<Int>?) {
        println("Below message are part of Kafka offset $offset")
        messages?.forEach { println(it)}
    }

}