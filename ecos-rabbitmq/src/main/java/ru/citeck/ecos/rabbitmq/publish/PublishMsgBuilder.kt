package ru.citeck.ecos.rabbitmq.publish

import java.time.Duration

/**
 * A builder interface for constructing and publishing messages to RabbitMQ.
 */
interface PublishMsgBuilder {

    /**
     * Specifies the queue where the message should be published.
     * Setting the queue will override the exchange to the default exchange
     * (empty string) and set the routing key to the queue name.
     */
    fun withQueue(queue: String): PublishMsgBuilder

    /**
     * Specifies the routing key for the message to be published.
     */
    fun withRoutingKey(routingKey: String): PublishMsgBuilder

    /**
     * Specifies the exchange to which the message will be published.
     */
    fun withExchange(exchange: String): PublishMsgBuilder

    /**
     * Sets the timeout duration for the publish operation. If a connection cannot be
     * established within the specified timeout duration, a `TimeoutException` will be thrown.
     */
    fun withPublishTimeout(timeout: Duration): PublishMsgBuilder

    /**
     * Sets the time-to-live (TTL) for the message in milliseconds. This determines how
     * long the message will be retained in the queue before it is discarded.
     * Zero value mean no TTL
     */
    fun withTtl(ttl: Long): PublishMsgBuilder

    /**
     * Sets the message payload to be published.
     */
    fun withMessage(message: Any): PublishMsgBuilder

    /**
     * Sets the headers for the message.
     */
    fun withHeaders(headers: Map<String, Any>): PublishMsgBuilder

    /**
     * Publishes the message with the specified parameters. This method finalizes
     * the building process and sends the message to RabbitMQ.
     */
    fun submit()
}
