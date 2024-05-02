package ru.citeck.ecos.rabbitmq

import com.rabbitmq.client.*
import mu.KotlinLogging
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.utils.NameUtils
import ru.citeck.ecos.commons.utils.func.UncheckedBiConsumer
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.rabbitmq.ack.AckedMessage
import ru.citeck.ecos.rabbitmq.ack.AckedMessageImpl
import ru.citeck.ecos.rabbitmq.obs.RmqConsumeObsContext
import ru.citeck.ecos.rabbitmq.obs.RmqPublishObsContext
import java.io.InputStream

class RabbitMqChannel(
    private val channel: Channel,
    private val context: RabbitMqConnCtx,
    private val micrometerContext: EcosMicrometerContext
) {

    companion object {
        val log = KotlinLogging.logger {}
        const val PARSING_ERRORS_QUEUE = "ecos.msg.parsing-errors.queue"

        private const val HEADER_MM_SCOPE = "ECOS-RMQ-Micrometer-Scope"

        // message doesn't survive in any queue after rabbit restart
        private const val DELIVERY_MODE_NON_PERSISTENT = 1
        // message survive in durable queue after rabbit restart
        private const val DELIVERY_MODE_PERSISTENT = 2

        val nameEscaper = NameUtils.getEscaperWithAllowedChars(".-:")

        private val strStrMapType = Json.mapper.getMapType(String::class.java, String::class.java)
    }

    init {
        declareQueue(PARSING_ERRORS_QUEUE, true)
    }

    fun cancelConsumer(tag: String) {
        channel.basicCancel(tag)
    }

    fun <T : Any> addConsumer(
        queue: String,
        msgType: Class<T>,
        action: (T, Map<String, Any>) -> Unit
    ): String {

        return addConsumer(
            queue, msgType,
            object : UncheckedBiConsumer<T, Map<String, Any>> {
                override fun accept(arg0: T, arg1: Map<String, Any>) {
                    return action.invoke(arg0, arg1)
                }
            }
        )
    }

    fun <T : Any> addAckedConsumer(
        queue: String,
        msgType: Class<T>,
        action: (AckedMessage<T>, Map<String, Any>) -> Unit
    ): String {

        return addAckedConsumer(
            queue, msgType,
            object : UncheckedBiConsumer<AckedMessage<T>, Map<String, Any>> {
                override fun accept(arg0: AckedMessage<T>, arg1: Map<String, Any>) {
                    return action.invoke(arg0, arg1)
                }
            }
        )
    }

    fun <T : Any> addAckedConsumer(
        queue: String,
        msgType: Class<T>,
        action: UncheckedBiConsumer<AckedMessage<T>, Map<String, Any>>
    ): String {

        return basicConsumeImpl(queue, false, msgType) { msg, headers, delivery ->

            val ackedMsg = AckedMessageImpl(msg, channel, delivery.envelope.deliveryTag)
            try {
                @Suppress("UNCHECKED_CAST")
                action.accept(ackedMsg, headers)
                ackedMsg.ack()
            } catch (e: Exception) {
                if (channel.isOpen) {
                    try {
                        ackedMsg.nack()
                    } catch (nackException: Exception) {
                        log.error(nackException) { "Exception while nack" }
                    }
                }
                throw e
            }
        }
    }

    fun <T : Any> addConsumer(
        queue: String,
        msgType: Class<T>,
        action: UncheckedBiConsumer<T, Map<String, Any>>
    ): String {

        return basicConsumeImpl(queue, true, msgType) { msg, headers, _ ->
            action.accept(msg, headers)
        }
    }

    private inline fun <T : Any> basicConsumeImpl(
        queue: String,
        autoAck: Boolean,
        msgType: Class<out T>,
        crossinline action: (T, Map<String, Any>, Delivery) -> Unit
    ): String {
        val escapedQueueName = nameEscaper.escape(queue)

        return channel.basicConsume(
            escapedQueueName,
            autoAck,
            { _, message: Delivery ->
                run {
                    val body: T? = if (msgType == Delivery::class.java) {
                        @Suppress("UNCHECKED_CAST")
                        message as T
                    } else {
                        context.fromMsgBodyBytes(message.body, msgType)
                    }
                    if (body == null) {
                        publishMsg(PARSING_ERRORS_QUEUE, ParsingError(message))
                    } else {
                        val headers = message.properties.headers ?: emptyMap()
                        val mmScope = parseStringStringMapHeader(headers[HEADER_MM_SCOPE])
                        micrometerContext.doWithinExtScope(mmScope) {
                            micrometerContext.createObs(
                                RmqConsumeObsContext(
                                    escapedQueueName,
                                    autoAck,
                                    msgType,
                                    this
                                )
                            ).observe {
                                action.invoke(body, headers, message)
                            }
                        }
                    }
                }
            },
            { consumerTag: String -> log.info("Consuming cancelled. Tag: $consumerTag") }
        )
    }

    private fun parseStringStringMapHeader(header: Any?): Map<String, String> {
        if (header == null) {
            return emptyMap()
        }
        return if (header is String && header.isNotBlank()) {
            Json.mapper.readNotNull(header, strStrMapType)
        } else if (header is LongString) {
            Json.mapper.readNotNull(header.stream, strStrMapType)
        } else if (header is InputStream) {
            Json.mapper.readNotNull(header, strStrMapType)
        } else if (header is ByteArray) {
            Json.mapper.readNotNull(header, strStrMapType)
        } else {
            emptyMap()
        }
    }

    @JvmOverloads
    fun publishMsg(
        queue: String,
        message: Any,
        headers: Map<String, Any> = emptyMap(),
        ttl: Long = 0L
    ) {

        publishMsg("", nameEscaper.escape(queue), message, headers, ttl)
    }

    @JvmOverloads
    fun publishMsg(
        exchange: String,
        routingKey: String,
        message: Any,
        headers: Map<String, Any> = emptyMap(),
        ttl: Long = 0L
    ) {

        val body = if (message is ByteArray) {
            message
        } else {
            context.toMsgBodyBytes(message)
        }

        micrometerContext.createObs(
            RmqPublishObsContext(
                exchange,
                routingKey,
                message,
                headers,
                ttl,
                this
            )
        ).observe {

            val mmScopeData = micrometerContext.extractScopeData()
            val msgHeaders = if (mmScopeData.isEmpty()) {
                headers
            } else {
                val fullHeaders = LinkedHashMap(headers)
                fullHeaders[HEADER_MM_SCOPE] = Json.mapper.toStringNotNull(mmScopeData)
                fullHeaders
            }

            val props = MessageProperties.MINIMAL_BASIC.builder()
            if (ttl > 0) {
                props.expiration(ttl.toString())
                if (ttl < (10 * 60 * 1000 /*10 min*/)) {
                    props.deliveryMode(DELIVERY_MODE_NON_PERSISTENT)
                } else {
                    props.deliveryMode(DELIVERY_MODE_PERSISTENT)
                }
            } else {
                props.deliveryMode(DELIVERY_MODE_PERSISTENT)
            }
            props.headers(msgHeaders)

            channel.basicPublish(nameEscaper.escape(exchange), routingKey, props.build(), body)
        }
    }

    fun declareQueue(queue: String, durable: Boolean) {

        val validQueue = nameEscaper.escape(queue)

        if (context.declaredQueues.add(validQueue)) {
            channel.queueDeclare(
                validQueue,
                durable,
                false,
                !durable,
                null
            )
        }
    }

    fun queueBind(queue: String, exchange: String, routingKey: String) {
        channel.queueBind(nameEscaper.escape(queue), nameEscaper.escape(exchange), routingKey)
    }

    fun declareExchange(exchange: String, type: BuiltinExchangeType, durable: Boolean) {

        val fixedExchange = nameEscaper.escape(exchange)

        if (context.declaredExchanges.add(fixedExchange)) {
            channel.exchangeDeclare(
                fixedExchange,
                type,
                durable,
                !durable,
                false,
                emptyMap()
            )
        }
    }

    fun close() {
        channel.close()
    }

    class ParsingError(
        val message: Delivery
    )
}
