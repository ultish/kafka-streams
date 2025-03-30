package jxhui

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.ValueAndTimestamp
import org.apache.kafka.streams.state.WindowStore
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant
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
            val singleKeyStream = input.selectKey { _, _ -> "all-messages" }

            val storeName = "windowed-store"
            val windowed = singleKeyStream
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(30), Duration.ofSeconds(5)))
                .aggregate(
                    { mutableListOf<String>() },
                    { _: String, value: String, aggregate: MutableList<String> ->
                        aggregate.apply { add(value) }; aggregate
                    },
                    Materialized.`as`<String, MutableList<String>, WindowStore<Bytes, ByteArray>>(storeName)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(ListStringSerde())
                )

//            // Debug: Log aggregated results directly from KTable
//            windowed.toStream().foreach { windowedKey, batch ->
//                logger.info(
//                    "Aggregated window [${windowedKey.window().startTime()} - ${
//                        windowedKey.window().endTime()
//                    }]: $batch"
//                )
//            }


            val processedStream: KStream<String, MutableList<String>> = windowed.toStream().process(ProcessorSupplier {
                object :
                    Processor<org.apache.kafka.streams.kstream.Windowed<String>, MutableList<String>, String, MutableList<String>> {
                    private lateinit var context: ProcessorContext<String, MutableList<String>>
                    private lateinit var store: WindowStore<String, MutableList<String>>

                    override fun init(context: ProcessorContext<String, MutableList<String>>) {
                        this.context = context
                        store = context.getStateStore(storeName) as WindowStore<String, MutableList<String>>
//                        logger.info("Processor initialized, store: $storeName")

                        context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME) { timestamp ->
                            val now = Instant.ofEpochMilli(timestamp)
//                            logger.info("Punctuator triggered at $now")

                            store.all().use { iterator ->
                                if (!iterator.hasNext()) {
//                                    logger.info("Store is empty")
                                }
                                while (iterator.hasNext()) {
                                    val entry = iterator.next()
                                    val windowStart = entry.key.window().start()
                                    val windowEnd = entry.key.window().end()
                                    val windowStartTime = Instant.ofEpochMilli(windowStart)
                                    val windowEndTime = Instant.ofEpochMilli(windowEnd)
                                    val valueAndTimestamp = entry.value as? ValueAndTimestamp<MutableList<String>>
                                    val batch = valueAndTimestamp?.value() ?: mutableListOf()
                                    logger.info("Checking window [$windowStartTime - $windowEndTime] with value: $batch")

                                    if (windowEnd + 5000L <= timestamp) {
                                        val originalKey = entry.key.key()
//                                        logger.info("Forwarding window [$windowEndTime]: $batch for key $originalKey")
                                        context.forward(Record(originalKey, batch, windowEnd))
                                        store.put(originalKey, null, windowStart) // Remove at window start
                                        logger.info("Removed window [$windowStartTime - $windowEndTime] for key $originalKey")
                                    } else {
//                                        logger.info("Window [$windowStartTime - $windowEndTime] not yet closed (now: $now)")
                                    }
                                }
                            }
                        }
                    }

                    override fun process(record: Record<org.apache.kafka.streams.kstream.Windowed<String>, MutableList<String>>?) {
//                        logger.info("Processing record: $record")
                    }
                }
            }, storeName)


            processedStream.foreach { key, batch ->
                logger.info("Window emitted for key $key: $batch")
            }
        }
    }
}
