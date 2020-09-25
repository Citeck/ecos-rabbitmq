package ru.citeck.ecos.rabbitmq

import com.rabbitmq.client.ConnectionFactory
import mu.KotlinLogging

class RabbitMqConnFactory {

    companion object {
        val log = KotlinLogging.logger {}
    }

    fun createConnection(props: RabbitMqConnProps) : RabbitMqConn? {

        val host = props.host

        if (host != null && host.isBlank()) {
            log.error { "RabbitMq host is empty. Props: $props" }
            return null
        }

        val connectionFactory = ConnectionFactory()
        connectionFactory.isAutomaticRecoveryEnabled = true
        connectionFactory.host = props.host
        connectionFactory.username = props.username
        connectionFactory.password = props.password

        if (props.virtualHost != null) {
            connectionFactory.virtualHost = props.virtualHost
        }
        val port = props.port
        if (port != null) {
            connectionFactory.port = port
        }

        return RabbitMqConn(connectionFactory, 10_000)
    }
}