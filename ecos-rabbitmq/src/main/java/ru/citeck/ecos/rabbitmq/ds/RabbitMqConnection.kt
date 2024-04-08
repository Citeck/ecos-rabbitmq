package ru.citeck.ecos.rabbitmq.ds

import com.rabbitmq.client.Connection
import ru.citeck.ecos.rabbitmq.RabbitMqChannel
import java.util.function.Consumer

interface RabbitMqConnection {

    fun waitUntilReady(timeoutMs: Long)

    fun doWithNewChannel(action: Consumer<RabbitMqChannel>)

    fun doWithNewChannel(qos: Int, action: Consumer<RabbitMqChannel>)

    fun doWithConnection(action: Consumer<Connection>)
}
