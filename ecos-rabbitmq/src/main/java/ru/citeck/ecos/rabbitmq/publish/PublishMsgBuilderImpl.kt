package ru.citeck.ecos.rabbitmq.publish

import ru.citeck.ecos.rabbitmq.RabbitMqChannel
import java.time.Duration

class PublishMsgBuilderImpl(
    private val submitAction: (PublishMsgBuilderImpl) -> Unit
) : PublishMsgBuilder {

    lateinit var message: Any

    var exchange: String = ""
    var routingKey: String = ""
    var headers: Map<String, Any> = emptyMap()

    var publishTimeout: Duration = Duration.ofMinutes(1)
    var ttl: Long = 0

    override fun withQueue(queue: String): PublishMsgBuilder {
        this.exchange = ""
        this.routingKey = RabbitMqChannel.nameEscaper.escape(queue)
        return this
    }

    override fun withRoutingKey(routingKey: String): PublishMsgBuilder {
        this.routingKey = routingKey
        return this
    }

    override fun withExchange(exchange: String): PublishMsgBuilder {
        this.exchange = exchange
        return this
    }

    override fun withPublishTimeout(timeout: Duration): PublishMsgBuilder {
        this.publishTimeout = timeout
        return this
    }

    override fun withTtl(ttl: Long): PublishMsgBuilder {
        this.ttl = ttl
        return this
    }

    override fun withMessage(message: Any): PublishMsgBuilder {
        this.message = message
        return this
    }

    override fun withHeaders(headers: Map<String, Any>): PublishMsgBuilder {
        this.headers = headers
        return this
    }

    override fun submit() {
        submitAction.invoke(this)
    }
}
