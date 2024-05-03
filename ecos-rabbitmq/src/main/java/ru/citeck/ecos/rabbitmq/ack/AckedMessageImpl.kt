package ru.citeck.ecos.rabbitmq.ack

import com.rabbitmq.client.Channel
import mu.KotlinLogging
import java.util.concurrent.atomic.AtomicBoolean

class AckedMessageImpl<T : Any>(
    private val content: T,
    private val channel: Channel,
    private val deliveryTag: Long
) : AckedMessage<T> {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private var wasAcked = AtomicBoolean(false)

    override fun getContent(): T {
        return content
    }

    override fun ack() {
        processAckOrNack(true)
    }

    override fun nack() {
        processAckOrNack(false)
    }

    private fun processAckOrNack(ack: Boolean) {
        if (wasAcked.compareAndSet(false, true)) {
            val operationName = if (ack) {
                "ack"
            } else {
                "nack"
            }
            if (channel.isOpen) {
                if (ack) {
                    try {
                        channel.basicAck(deliveryTag, false)
                    } catch (e: Throwable) {
                        wasAcked.set(false)
                        if (e is InterruptedException) {
                            Thread.currentThread().interrupt()
                        }
                        throw e
                    }
                } else {
                    try {
                        channel.basicNack(deliveryTag, false, true)
                    } catch (e: Throwable) {
                        // if channel was closed, then log message doesn't helpful
                        if (channel.isOpen) {
                            log.warn(e) { "Exception while nack message. Delivery tag: $deliveryTag" }
                        }
                    }
                }
            } else {
                log.debug {
                    "Channel already closed and message $operationName " +
                    "won't be executed. Delivery tag: $deliveryTag"
                }
            }
        }
    }
}
