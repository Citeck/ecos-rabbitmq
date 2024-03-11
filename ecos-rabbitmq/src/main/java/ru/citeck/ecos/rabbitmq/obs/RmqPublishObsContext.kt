package ru.citeck.ecos.rabbitmq.obs

import ru.citeck.ecos.micrometer.obs.EcosObsContext
import ru.citeck.ecos.rabbitmq.RabbitMqChannel

class RmqPublishObsContext(
    val exchange: String,
    val routingKey: String,
    val message: Any,
    val headers: Map<String, Any> = emptyMap(),
    val ttl: Long = 0L,
    val channel: RabbitMqChannel
) : EcosObsContext(NAME) {
    companion object {
        const val NAME = "ecos.rabbitmq.publish"
    }
}
