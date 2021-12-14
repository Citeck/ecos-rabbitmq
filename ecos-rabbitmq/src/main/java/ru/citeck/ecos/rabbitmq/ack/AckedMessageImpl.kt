package ru.citeck.ecos.rabbitmq.ack

import com.rabbitmq.client.Channel

class AckedMessageImpl<T : Any>(
    private val content: T,
    private val channel: Channel,
    private val deliveryTag: Long
) : AckedMessage<T> {

    private var wasAcked = false

    override fun getContent(): T {
        return content
    }

    override fun ack() {
        if (!wasAcked) {
            channel.basicAck(deliveryTag, false)
            wasAcked = true
        }
    }

    override fun nack() {
        if (!wasAcked) {
            channel.basicNack(deliveryTag, false, true)
            wasAcked = true
        }
    }
}
