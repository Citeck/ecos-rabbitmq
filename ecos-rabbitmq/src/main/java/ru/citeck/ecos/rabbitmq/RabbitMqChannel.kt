package ru.citeck.ecos.rabbitmq

import com.google.common.io.BaseEncoding
import com.google.common.primitives.Longs
import com.rabbitmq.client.*
import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.crc.CRC64
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.utils.ExceptionUtils
import ru.citeck.ecos.commons.utils.NameUtils
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.rabbitmq.ack.AckedMessage
import ru.citeck.ecos.rabbitmq.ack.AckedMessageImpl
import ru.citeck.ecos.rabbitmq.obs.RmqConsumeObsContext
import ru.citeck.ecos.rabbitmq.obs.RmqPublishObsContext
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.webapp.api.func.UncheckedBiConsumer
import java.io.InputStream
import java.security.SecureRandom
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.HashMap
import kotlin.collections.LinkedHashMap

class RabbitMqChannel(
    private val channel: Channel,
    private val context: RabbitMqConnCtx,
    private val micrometerContext: EcosMicrometerContext,
    private val webAppApi: EcosWebAppApi? = null
) {

    companion object {
        private val log = KotlinLogging.logger {}

        const val PARSING_ERRORS_QUEUE = "ecos.msg.parsing-errors.queue"
        const val DLQ_POSTFIX = "-dlq"
        const val RETRY_POSTFIX = "-retry"

        const val X_EXCEPTION_STACKTRACE: String = "x-exception-stacktrace"
        const val X_EXCEPTION_MESSAGE: String = "x-exception-message"

        private const val RETRY_COUNT_HEADER = "retry-count"

        private const val HEADER_MM_SCOPE = "ECOS-RMQ-Micrometer-Scope"

        // message doesn't survive in any queue after rabbit restart
        private const val DELIVERY_MODE_NON_PERSISTENT = 1

        // message survive in durable queue after rabbit restart
        private const val DELIVERY_MODE_PERSISTENT = 2

        val nameEscaper = NameUtils.getEscaperWithAllowedChars(".-:")

        private val strStrMapType = Json.mapper.getMapType(String::class.java, String::class.java)
    }

    private val activeConsumers = Collections.newSetFromMap<String>(ConcurrentHashMap())

    init {
        declareQueue(PARSING_ERRORS_QUEUE, true)
    }

    fun getOriginalChannel(): Channel {
        return channel
    }

    @Synchronized
    fun cancelConsumer(tag: String) {
        activeConsumers.remove(tag)
        doWithoutAlreadyClosedException {
            channel.basicCancel(tag)
        }
    }

    /* ADD CONSUMER */

    @Synchronized
    fun <T : Any> addConsumer(
        queue: String,
        msgType: Class<T>,
        action: (T, Map<String, Any>) -> Unit
    ): String {

        return addConsumer(
            queue,
            msgType,
            object : UncheckedBiConsumer<T, Map<String, Any>> {
                override fun accept(arg0: T, arg1: Map<String, Any>) {
                    return action.invoke(arg0, arg1)
                }
            }
        )
    }

    /**
     * Adds a consumer to the specified queue with retrying mechanism [declareQueuesWithRetrying].
     *
     * This method creates a consumer that will consume messages from the specified queue. If an exception
     * is thrown during the processing of a message, the message will be sent to a retry queue and retried
     * up to a maximum number of times specified by [maxRetryCount]. If the maximum retry count is reached,
     * the message will be sent to a dead letter queue.
     *
     * @param queue The name of the queue to consume messages from.
     * @param msgType The type of the messages to consume. This is used for deserializing the message body.
     * @param maxRetryCount The maximum number of times to retry processing a message if an exception is thrown.
     * @param action The action to perform for each consumed message. This action is a function that takes
     *               the message and a map of message headers as parameters.
     *
     * @return The consumer tag for the new consumer.
     */
    @Synchronized
    fun <T : Any> addConsumerWithRetrying(
        queue: String,
        msgType: Class<T>,
        maxRetryCount: Int,
        action: (AckedMessage<T>, Map<String, Any>) -> Unit
    ): String {

        val validQueue = nameEscaper.escape(queue)
        val retryQueue = "$validQueue$RETRY_POSTFIX"
        val dlq = "$validQueue$DLQ_POSTFIX"

        return basicConsumeImpl(queue, false, msgType) { msg, headers, delivery ->
            val ackedMsg = AckedMessageImpl(msg, channel, delivery.envelope.deliveryTag)

            try {
                action.invoke(ackedMsg, headers)
                ackedMsg.ack()
                log.trace {
                    val retryCount = headers.getOrDefault(RETRY_COUNT_HEADER, 0) as Int
                    "Message processed successfully. Retry count: $retryCount, Msg: $msg"
                }
            } catch (e: Exception) {
                val rootCause = ExceptionUtils.getRootCause(e)
                val retryCount = headers.getOrDefault(RETRY_COUNT_HEADER, 0) as Int

                val headersWithMeta = HashMap(headers)
                headersWithMeta[X_EXCEPTION_STACKTRACE] = rootCause.stackTraceToString()
                headersWithMeta[X_EXCEPTION_MESSAGE] = rootCause.message ?: ""

                if (retryCount >= maxRetryCount) {
                    log.error(e) {
                        "Failed process msg. Max retry count reached of $maxRetryCount. Sending to DLQ $dlq, " +
                            "Msg: $msg"
                    }

                    headersWithMeta.remove(RETRY_COUNT_HEADER)
                    publishMsg(dlq, msg, headersWithMeta)
                } else {
                    log.debug(e) {
                        "Failed process msg. Send message to retrying queue $retryQueue. Retry count: $retryCount, " +
                            "Msg: $msg"
                    }

                    headersWithMeta[RETRY_COUNT_HEADER] = retryCount + 1
                    publishMsg(retryQueue, msg, headersWithMeta)
                }

                ackedMsg.ack()
            }
        }
    }

    @Synchronized
    fun <T : Any> addAckedConsumer(
        queue: String,
        msgType: Class<T>,
        action: (AckedMessage<T>, Map<String, Any>) -> Unit
    ): String {

        return addAckedConsumer(
            queue,
            msgType,
            object : UncheckedBiConsumer<AckedMessage<T>, Map<String, Any>> {
                override fun accept(arg0: AckedMessage<T>, arg1: Map<String, Any>) {
                    return action.invoke(arg0, arg1)
                }
            }
        )
    }

    @Synchronized
    fun <T : Any> addAckedConsumer(
        queue: String,
        msgType: Class<T>,
        action: UncheckedBiConsumer<AckedMessage<T>, Map<String, Any>>
    ): String {

        return basicConsumeImpl(queue, false, msgType) { msg, headers, delivery ->

            val ackedMsg = AckedMessageImpl(msg, channel, delivery.envelope.deliveryTag)
            try {
                action.accept(ackedMsg, headers)
                ackedMsg.ack()
            } catch (e: Exception) {
                ackedMsg.nack()
                if (e is InterruptedException) {
                    Thread.currentThread().interrupt()
                }
                throw e
            }
        }
    }

    @Synchronized
    fun <T : Any> addConsumer(
        queue: String,
        msgType: Class<T>,
        action: UncheckedBiConsumer<T, Map<String, Any>>
    ): String {

        return basicConsumeImpl(queue, true, msgType) { msg, headers, _ ->
            action.accept(msg, headers)
        }
    }

    @Synchronized
    private fun <T : Any> basicConsumeImpl(
        queue: String,
        autoAck: Boolean,
        msgType: Class<out T>,
        action: (T, Map<String, Any>, Delivery) -> Unit
    ): String {

        val escapedQueueName = nameEscaper.escape(queue)
        val consumerTag = generateConsumerTag()
        if (consumerTag.isNotBlank()) {
            activeConsumers.add(consumerTag)
        }

        fun processMsg(message: Delivery) {
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

        fun startConsuming(): String {
            return channel.basicConsume(
                escapedQueueName,
                autoAck,
                consumerTag,
                { _, message: Delivery -> processMsg(message) },
                { cancelledConsumerTag: String -> log.info("Consuming cancelled. Tag: $cancelledConsumerTag") }
            )
        }

        return if (webAppApi == null) {
            val tag = startConsuming()
            activeConsumers.add(tag)
            return tag
        } else {
            webAppApi.doWhenAppReady {
                if (activeConsumers.contains(consumerTag)) {
                    startConsuming()
                }
            }
            consumerTag
        }
    }

    private fun generateConsumerTag(): String {

        val props = webAppApi?.getProperties() ?: return ""

        val crc64 = CRC64()

        val rnd = SecureRandom()
        val bytes = ByteArray(1024)
        rnd.nextBytes(bytes)
        crc64.update(bytes)

        val rawId = BaseEncoding.base32().encode(Longs.toByteArray(crc64.value))
        val uniqueId = rawId.lowercase().substringBefore('=').takeLast(12)

        return "amq.webapp.${props.appName}.${props.appInstanceId}-$uniqueId"
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

    @Synchronized
    @JvmOverloads
    fun publishMsg(
        queue: String,
        message: Any,
        headers: Map<String, Any> = emptyMap(),
        ttl: Long = 0L
    ) {

        publishMsg("", nameEscaper.escape(queue), message, headers, ttl)
    }

    @Synchronized
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

    @Synchronized
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

    /**
     * Declare queues with retrying mechanism:
     * - [mainQueue] - main queue
     * - [mainQueue]-retry - queue for retrying messages
     * - [mainQueue]-dlq - dead letter queue
     *
     * If message processing failed, it will be sent to retry queue, message will be delayed
     * for [retryDelayMs] milliseconds. If message retry count exceeds maxRetryCount of [addConsumerWithRetrying] method,
     * message will be sent to DLQ.
     *
     * What doing with DLQ messages - you should decide in your implementation
     *
     * @param mainQueue queue name
     * @param retryDelayMs delay between retries in milliseconds. Actually it's TTL for retry queue. Be careful to
     * change it, because rabbitmq does not support modifying arguments on existing queue. If you want to change it,
     * you should delete queue and create it again, otherwise it will be error on queue declare.
     */
    @Synchronized
    fun declareQueuesWithRetrying(
        mainQueue: String,
        retryDelayMs: Long,
        durable: Boolean = true
    ) {
        val validQueue = nameEscaper.escape(mainQueue)
        val retryQueue = "$mainQueue$RETRY_POSTFIX"
        val deadLetterQueue = "$mainQueue$DLQ_POSTFIX"

        if (context.declaredQueues.add(validQueue)) {
            val args = HashMap<String, Any>()
            args["x-dead-letter-exchange"] = ""
            args["x-dead-letter-routing-key"] = retryQueue
            channel.queueDeclare(validQueue, durable, false, !durable, args)
        }

        if (context.declaredQueues.add(retryQueue)) {
            val args = HashMap<String, Any>()
            args["x-dead-letter-exchange"] = ""
            args["x-dead-letter-routing-key"] = validQueue
            args["x-message-ttl"] = retryDelayMs
            channel.queueDeclare(retryQueue, durable, false, !durable, args)
        }

        if (context.declaredQueues.add(deadLetterQueue)) {
            channel.queueDeclare(deadLetterQueue, durable, false, !durable, null)
        }
    }

    @Synchronized
    fun queueBind(queue: String, exchange: String, routingKey: String) {
        channel.queueBind(nameEscaper.escape(queue), nameEscaper.escape(exchange), routingKey)
    }

    @Synchronized
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

    @Synchronized
    fun close() {
        activeConsumers.clear()
        doWithoutAlreadyClosedException {
            channel.close()
        }
    }

    fun isOpen(): Boolean {
        return channel.isOpen
    }

    /**
     * Method required to perform cancel/close action without exception about channel or connection is closed.
     * Simple check if (isOpen) { action.invoke() } doesn't help because required logic
     * in AutorecoveringChannel and AutorecoveringConnection won't be executed and consumer or channel
     * may be unexpectedly recovered.
     */
    private fun doWithoutAlreadyClosedException(action: () -> Unit) {
        try {
            return action.invoke()
        } catch (e: AlreadyClosedException) {
            log.debug { "Already closed exception. Msg: ${e.message}" }
        }
    }

    class ParsingError(
        val message: Delivery
    )
}
