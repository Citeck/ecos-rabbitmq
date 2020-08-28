package ru.citeck.ecos.rabbitmq.spring

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties("spring.rabbitmq")
class RabbitMqConnectionProperties {

    var host: String? = null
    var username: String? = null
    var password: String? = null
}
