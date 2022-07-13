package com.experiments.kafka.tutorial01

import com.experiments.kafka.utils.KafkaServerProperties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class ProducerDemo {
}

fun main(args: Array<String>) {
    println("---- Producer Demo ----")

    val producer = KafkaProducer<String, String>(KafkaServerProperties.loadProducerConfig())

    // create producer record
    val record = ProducerRecord<String, String>("dummy-java-topic", "hello java2")

    // send data -async
    producer.send(record)

    // flush data
    producer.flush()
    // flush and close producer
    producer.close()
}
