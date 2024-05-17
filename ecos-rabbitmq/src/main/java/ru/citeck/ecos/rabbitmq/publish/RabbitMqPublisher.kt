package ru.citeck.ecos.rabbitmq.publish

/**
 * Interface representing a RabbitMQ publisher.
 */
interface RabbitMqPublisher {

    /**
     * Publishes a message to the specified queue.
     *
     * @param queue The name of the queue to publish the message to.
     * @param message The message to be published.
     */
    fun publishMsg(queue: String, message: Any) {
        publishMsg()
            .withQueue(queue)
            .withMessage(message)
            .submit()
    }

    /**
     * Publishes a message to the specified queue with additional headers.
     *
     * @param queue The name of the queue to publish the message to.
     * @param message The message to be published.
     * @param headers Additional headers for the message.
     */
    fun publishMsg(queue: String, message: Any, headers: Map<String, Any>) {
        publishMsg()
            .withQueue(queue)
            .withMessage(message)
            .withHeaders(headers)
            .submit()
    }

    /**
     * Publishes a message to the specified exchange with the given routing key.
     *
     * @param exchange The name of the exchange to publish the message to.
     * @param routingKey The routing key to use for the message.
     * @param message The message to be published.
     */
    fun publishMsg(exchange: String, routingKey: String, message: Any) {
        publishMsg()
            .withExchange(exchange)
            .withRoutingKey(routingKey)
            .withMessage(message)
            .submit()
    }

    /**
     * Publishes a message to the specified exchange with the given routing key and additional headers.
     *
     * @param exchange The name of the exchange to publish the message to.
     * @param routingKey The routing key to use for the message.
     * @param message The message to be published.
     * @param headers Additional headers for the message.
     */
    fun publishMsg(exchange: String, routingKey: String, message: Any, headers: Map<String, Any>) {
        publishMsg()
            .withExchange(exchange)
            .withRoutingKey(routingKey)
            .withMessage(message)
            .withHeaders(headers)
            .submit()
    }

    /**
     * Returns a new instance of [PublishMsgBuilder] to configure and publish a message.
     *
     * @return An instance of [PublishMsgBuilder].
     */
    fun publishMsg(): PublishMsgBuilder
}
