package ru.citeck.ecos.rabbitmq

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Delivery
import mu.KotlinLogging
import ru.citeck.ecos.commons.utils.NameUtils
import ru.citeck.ecos.commons.utils.func.UncheckedBiConsumer

class RabbitMqChannel(private val channel: Channel,
                      private val context: RabbitMqConnCtx) {

    companion object {
        val log = KotlinLogging.logger {}
        const val PARSING_ERRORS_QUEUE = "ecos.msg.parsing-errors.queue"
        val nameEscaper = NameUtils.getEscaperWithAllowedChars(".-:")
    }

    init {
        declareQueue(PARSING_ERRORS_QUEUE, true)
    }

    fun <T : Any> addConsumer(queue: String,
                              msgType: Class<T>,
                              action: (T, Map<String, Any>) -> Unit) : String {

        return addConsumer(nameEscaper.escape(queue), msgType, object : UncheckedBiConsumer<T, Map<String, Any>> {
            override fun accept(arg0: T, arg1: Map<String, Any>) {
                return action.invoke(arg0, arg1)
            }
        })
    }

    fun <T : Any> addConsumer(queue: String,
                              msgType: Class<T>,
                              action: UncheckedBiConsumer<T, Map<String, Any>>) : String {

        return channel.basicConsume(
            nameEscaper.escape(queue),
            true,
            { _, message: Delivery ->
                run {
                    val body = context.fromMsgBodyBytes(message.body, msgType)
                    if (body == null) {
                        publishMsg(PARSING_ERRORS_QUEUE, ParsingError(message))
                    } else {
                        val headers = message.properties.headers ?: emptyMap()
                        action.accept(body, headers)
                    }
                }
            },
            { consumerTag: String -> log.info("Consuming cancelled. Tag: $consumerTag") }
        )
    }

    @JvmOverloads
    fun publishMsg(queue: String,
                   message: Any,
                   headers: Map<String, Any> = emptyMap(),
                   ttl: Long = 0L) {

        publishMsg("", nameEscaper.escape(queue), message, headers, ttl)
    }

    @JvmOverloads
    fun publishMsg(exchange: String,
                   routingKey: String,
                   message: Any,
                   headers: Map<String, Any> = emptyMap(),
                   ttl: Long = 0L) {

        val body = context.toMsgBodyBytes(message)

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

    class ParsingError(
        val message: Delivery
    )
}
