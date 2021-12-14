package ru.citeck.ecos.rabbitmq.ack

interface AckedMessage<T : Any> {

    fun getContent(): T

    fun ack()

    fun nack()
}
