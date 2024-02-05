package ru.citeck.ecos.rabbitmq

import com.rabbitmq.client.ConnectionFactory
import mu.KotlinLogging
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import java.util.concurrent.ExecutorService

class RabbitMqConnFactory {

    companion object {
        val log = KotlinLogging.logger {}
    }

    private var micrometerContext: EcosMicrometerContext = EcosMicrometerContext.NOOP

    fun init(micrometerContext: EcosMicrometerContext = EcosMicrometerContext.NOOP) {
        this.micrometerContext = micrometerContext
    }

    @JvmOverloads
    fun createConnection(
        props: RabbitMqConnProps,
        executor: ExecutorService? = null,
        initDelayMs: Long = 10_000
    ): RabbitMqConn? {

        val host = props.host

        if (host.isBlank()) {
            log.error { "RabbitMq host is empty. Props: $props" }
            return null
        }

        val connectionFactory = ConnectionFactory()
        connectionFactory.isAutomaticRecoveryEnabled = true
        connectionFactory.host = props.host
        connectionFactory.username = props.username
        connectionFactory.password = props.password
        connectionFactory.virtualHost = props.virtualHost

        val port = props.port
        if (port != null) {
            connectionFactory.port = port
        }

        return RabbitMqConn(connectionFactory, executor, initDelayMs, micrometerContext)
    }
}
