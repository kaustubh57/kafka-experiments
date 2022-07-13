package com.experiments.kafka.tutorial01

import com.experiments.kafka.utils.KafkaServerProperties
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import java.time.Duration

class ConsumerDemo {
}

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(ConsumerDemo::class.java)

    println("---- ConsumerDemo Demo ----")

    val consumer = KafkaConsumer<String, String>(KafkaServerProperties.loadConsumerConfig())
    val topic1 = "dummy-java-topic"
    val topic2 = "dummy-java-topic-with-keys"

    consumer.subscribe(listOf(topic1, topic2))

    while (true) {
        val records = consumer.poll(Duration.ofMillis(100))

        for (record in records) {
            logger.info("--- TOPIC - ${record.topic()} ---")
            logger.info("Key: ${record.key()}, Value: ${record.value()}")
            logger.info("Partition: ${record.partition()}, Offset: ${record.offset()}")
        }
    }
}
