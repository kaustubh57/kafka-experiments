package com.experiments.kafka.tutorial01

import com.experiments.kafka.utils.KafkaServerProperties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory

class ProducerDemoWithKeys {
}

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(ProducerDemoWithKeys::class.java)

    println("---- ProducerDemoWithKeys Demo ----")

    val producer = KafkaProducer<String, String>(KafkaServerProperties.loadProducerConfig())
    val topic = "dummy-java-topic-with-keys"

    for (i in 1..5) {
        val value = "hello java with keys $i"
        val key = "id_$i"
        // create producer record
        val record = ProducerRecord<String, String>(topic, key, value)

        logger.info("Key: $key")

        // send data -async
        producer.send(record) { recordMetadata: RecordMetadata, exception: Exception? ->
            // executes every time a record is successfully sent or an exception is thrown
            if (exception == null) {
                logger.info(
                    "Receive new metadata: \n" +
                            "Topic: ${recordMetadata.topic()} \n" +
                            "Partition: ${recordMetadata.partition()} \n" +
                            "Offset: ${recordMetadata.offset()} \n" +
                            "Timestamp: ${recordMetadata.timestamp()} \n"
                )
            } else {
                logger.error("Error occurred while producing", exception)
            }
        }.get() // blocking call - don't do this is PROD
    }

    // flush data
    producer.flush()
    // flush and close producer
    producer.close()
}
