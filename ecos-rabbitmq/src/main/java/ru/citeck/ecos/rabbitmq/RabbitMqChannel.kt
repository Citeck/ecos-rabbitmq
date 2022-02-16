package ru.citeck.ecos.rabbitmq

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Delivery
import mu.KotlinLogging
import ru.citeck.ecos.commons.utils.NameUtils
import ru.citeck.ecos.commons.utils.func.UncheckedBiConsumer
import ru.citeck.ecos.rabbitmq.ack.AckedMessage
import ru.citeck.ecos.rabbitmq.ack.AckedMessageImpl

class RabbitMqChannel(
    private val channel: Channel,
    private val context: RabbitMqConnCtx
) {

    companion object {
        val log = KotlinLogging.logger {}
        const val PARSING_ERRORS_QUEUE = "ecos.msg.parsing-errors.queue"
        val nameEscaper = NameUtils.getEscaperWithAllowedChars(".-:")
    }

    init {
        declareQueue(PARSING_ERRORS_QUEUE, true)
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
        msgType: Class<T>,
        crossinline action: (T, Map<String, Any>, Delivery) -> Unit
    ): String {

        return channel.basicConsume(
            nameEscaper.escape(queue),
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
                        action.invoke(body, headers, message)
                    }
                }
            },
            { consumerTag: String -> log.info("Consuming cancelled. Tag: $consumerTag") }
        )
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

        val props = AMQP.BasicProperties.Builder().headers(headers)
        if (ttl > 0) {
            props.expiration(ttl.toString())
        }
        channel.basicPublish(nameEscaper.escape(exchange), routingKey, props.build(), body)
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
