package ru.citeck.ecos.commons.rabbit

import ecos.com.fasterxml.jackson210.dataformat.cbor.CBORFactory
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.json.JsonOptions
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

class EcosRabbitConnectionCtx(val connection: EcosRabbitConnection) {

    val declaredQueues: MutableSet<String> = Collections.newSetFromMap(ConcurrentHashMap())
    val declaredExchanges: MutableSet<String> = Collections.newSetFromMap(ConcurrentHashMap())

    private val msgBodyMapper = Json.newMapper(JsonOptions.create {
        setFactory(CBORFactory())
    })

    fun toMsgBodyBytes(data: Any) : ByteArray {

        val baos = ByteArrayOutputStream()

        GZIPOutputStream(baos).use {
            msgBodyMapper.write(it, data)
        }
        return baos.toByteArray()
    }

    fun <T : Any> fromMsgBodyBytes(bytes: ByteArray, type: Class<T>) : T? {
        val input = GZIPInputStream(ByteArrayInputStream(bytes))
        return msgBodyMapper.read(input, type)
    }
}
