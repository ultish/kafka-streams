package jxhui

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.TimeWindows
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.function.Consumer

@SpringBootApplication
class Main

fun main(args: Array<String>) {
    runApplication<Main>(*args)
}

@Service
class KafkaStreamsApplication {
    private val logger = LoggerFactory.getLogger(KafkaStreamsApplication::class.java)
    private val objectMapper: ObjectMapper = ObjectMapper()


    private fun listSerde(): Serde<MutableList<String>> {
        return Serdes.serdeFrom(
            { _, list -> objectMapper.writeValueAsBytes(list) }, // Serializer: List<String> to byte array, ignoring topic
            { _, bytes ->
                objectMapper.readValue(
                    bytes,
                    objectMapper.typeFactory.constructCollectionType(List::class.java, String::class.java)
                )
            } // Deserializer: byte array to List<String>
        )
    }

    @Bean
    fun process(): Consumer<KStream<String, String>> {
        return Consumer { input ->
            // Create a single key for all messages
            val singleKeyStream = input
                .selectKey { _, _ -> "all-messages" }

            singleKeyStream
                .groupByKey()
                .windowedBy(
                    TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5))
                )
                .aggregate(
                    { mutableListOf<String>() }, // Initializer
                    { _, value, aggregate ->
                        aggregate.apply { add(value) } // Aggregator: add each message to the list
                        aggregate // Return the updated list
                    },
                    Materialized.with(
                        Serdes.String(),
                        listSerde()
//                        ListStringSerde() // if you want full control of the serde, use this instead
                    )
                )
//                .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(10), Suppressed.BufferConfig.unbounded()))
//                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())) // Wait until window closes. the closing seems to only happen on reciept of a msg, so if you don't send anything it doesnt seem to close the last window.
                .toStream()
                .foreach { windowedKey, value ->
                    logger.info(
                        "Window [${windowedKey.window().startTime()} - ${windowedKey.window().endTime()}]: \n$value"
                    )
                }
        }
    }
}
