package com.experiments.kafka.utils

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

class KafkaServerProperties {
    companion object {
        private const val BOOTSTRAP_SERVER = "10.0.1.67:9092"
        private const val CONSUMER_GROUP_ID = "consumer-java-app"

        fun loadProducerConfig(): Properties {
            val properties = Properties()
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

            return properties
        }

        fun loadConsumerConfig(): Properties {
            val properties = Properties()
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID)
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

            return properties
        }
    }
}
