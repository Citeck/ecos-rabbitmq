package ru.citeck.ecos.rabbitmq

interface RabbitMqConnProvider {

    fun getConnection(): RabbitMqConn?
}
