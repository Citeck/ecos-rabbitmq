package ru.citeck.ecos.rabbitmq.spring

import com.rabbitmq.client.ConnectionFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.citeck.ecos.rabbitmq.EcosRabbitConnection
import ru.citeck.ecos.rabbitmq.EcosRabbitConnectionProvider

@Configuration
open class EcosRabbitConnectionConfig {

    @Bean
    @ConditionalOnMissingBean(EcosRabbitConnectionProvider::class)
    open fun getProvider(mqProps: RabbitMqConnectionProperties) : EcosRabbitConnectionProvider {
        return Provider(mqProps)
    }

    private class Provider(mqProps: RabbitMqConnectionProperties) : EcosRabbitConnectionProvider {

        private val connection: EcosRabbitConnection?

        init {
            val host = mqProps.host

            if (host != null && host.isNotBlank()) {
                val connectionFactory = ConnectionFactory()
                connectionFactory.isAutomaticRecoveryEnabled = true
                connectionFactory.host = mqProps.host
                connectionFactory.username = mqProps.username
                connectionFactory.password = mqProps.password

                connection = EcosRabbitConnection(connectionFactory, 10_000)
            } else {
                connection = null
            }
        }

        override fun getConnection() = connection
    }

}
