package ru.citeck.ecos.rabbitmq.spring

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.rabbitmq.RabbitMqConnFactory
import ru.citeck.ecos.rabbitmq.RabbitMqConnProvider
import ru.citeck.ecos.rabbitmq.RabbitMqConnProps

@Configuration
open class RabbitMqConnConfig {

    @Bean
    @ConditionalOnMissingBean(RabbitMqConnProvider::class)
    open fun getProvider(mqProps: RabbitMqConnProps) : RabbitMqConnProvider {
        return Provider(mqProps)
    }

    @Bean
    @ConfigurationProperties("spring.rabbitmq")
    @ConditionalOnMissingBean(RabbitMqConnProps::class)
    open fun getProperties() : RabbitMqConnProps {
        return RabbitMqConnProps();
    }

    private class Provider(mqProps: RabbitMqConnProps) : RabbitMqConnProvider {

        private val conn: RabbitMqConn? = RabbitMqConnFactory().createConnection(mqProps)

        override fun getConnection() = conn
    }
}
