package ru.citeck.ecos.rabbitmq.obs

import ru.citeck.ecos.micrometer.obs.EcosObsContext
import ru.citeck.ecos.rabbitmq.RabbitMqChannel

class RmqConsumeObsContext(
    val queue: String,
    val autoAck: Boolean,
    val msgType: Class<*>,
    val channel: RabbitMqChannel
) : EcosObsContext(NAME) {
    companion object {
        const val NAME = "ecos.rabbitmq.consume"
    }
}
