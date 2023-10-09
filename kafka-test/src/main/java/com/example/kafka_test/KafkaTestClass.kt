package com.example.kafka_test

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties

class KafkaTestClass {
    private fun createProducer(): KafkaProducer<String, String> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        return KafkaProducer(props)
    }

    fun sendToKafka(person: Person) {
        val producer = createProducer()

        val topic = "personDetails"

        //converting the object to json
        val personJson = """{"firstName":"${person.firstName}","lastName":"${person.lastName}","age":${person.age}}"""

        val record = ProducerRecord<String,String>(topic, personJson)
        producer.send(record)

        producer.close()

    }

}
