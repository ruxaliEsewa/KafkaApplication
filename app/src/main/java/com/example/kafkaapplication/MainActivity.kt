package com.example.kafkaapplication

import androidx.appcompat.app.AppCompatActivity
import android.os.Bundle
import com.example.kafkaapplication.databinding.ActivityMainBinding
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties


class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        binding.submit.setOnClickListener{
            val userFirstName = binding.firstName.text.toString()
            val userLastName = binding.lastName.text.toString()
            val userAge = binding.age.text.toString().toIntOrNull() ?: 0

            val person = Person(userFirstName,userLastName,userAge)
//            val kafkaTest = KafkaTestClass()
//            kafkaTest.sendToKafka(person)

            sendToKafka(person)
        }

    }

    private fun createProducer(): KafkaProducer<String, String> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        return KafkaProducer(props)
    }

    private fun sendToKafka(person: Person) {
        val producer = createProducer()

        val topic = "personDetails"

        //converting the object to json
        val personJson = """{"firstName":"${person.firstName}","lastName":"${person.lastName}","age":${person.age}}"""

        val record = ProducerRecord<String,String>(topic, personJson)
        producer.send(record)

        producer.close()

    }



}