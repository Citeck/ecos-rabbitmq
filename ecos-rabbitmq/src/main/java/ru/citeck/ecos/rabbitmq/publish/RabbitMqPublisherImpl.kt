package ru.citeck.ecos.rabbitmq.publish

import ru.citeck.ecos.rabbitmq.RabbitMqChannel
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class RabbitMqPublisherImpl(ecosConnection: RabbitMqConn) : RabbitMqPublisher {

    private val channel = CompletableFuture<RabbitMqChannel>()

    init {
        ecosConnection.doWithNewChannel {
            this.channel.complete(it)
        }
    }

    override fun publishMsg(): PublishMsgBuilder {
        return PublishMsgBuilderImpl {
            // todo: add pool of channels support
            val channel = channel.get(it.publishTimeout.toMillis(), TimeUnit.MILLISECONDS)
            channel.publishMsg(it.exchange, it.routingKey, it.message, it.headers, it.ttl)
        }
    }
}
