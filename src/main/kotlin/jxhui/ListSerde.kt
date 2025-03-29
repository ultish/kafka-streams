package jxhui

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class ListStringSerde : Serde<MutableList<String>> {
    private val objectMapper: ObjectMapper = ObjectMapper()
    private val serializer = Serializer<MutableList<String>> { _, data ->
        if (data == null) null else objectMapper.writeValueAsBytes(data)
    }
    private val deserializer = org.apache.kafka.common.serialization.Deserializer<MutableList<String>> { _, data ->
        if (data == null) null else objectMapper.readValue(
            data,
            objectMapper.typeFactory.constructCollectionType(MutableList::class.java, String::class.java)
        )
    }

    override fun serializer() = serializer
    override fun deserializer() = deserializer
}