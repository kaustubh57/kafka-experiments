package com.experiments.kafka.tutorial01

import com.experiments.kafka.utils.KafkaServerProperties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory

class ProducerDemo {
}

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(ProducerDemo::class.java)

    println("---- Producer Demo ----")

    val producer = KafkaProducer<String, String>(KafkaServerProperties.loadProducerConfig())

    for (i in 1..20) {
        // create producer record
        val record = ProducerRecord<String, String>("dummy-java-topic", "hello java $i")

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
        }
    }

    // flush data
    producer.flush()
    // flush and close producer
    producer.close()
}
