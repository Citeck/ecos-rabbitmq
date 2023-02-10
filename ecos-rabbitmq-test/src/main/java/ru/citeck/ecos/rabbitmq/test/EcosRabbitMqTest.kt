package ru.citeck.ecos.rabbitmq.test

import com.rabbitmq.client.ConnectionFactory
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.test.commons.listener.EcosTestExecutionListener

object EcosRabbitMqTest {

    private var connection: RabbitMqConn? = null

    fun createConnection(): RabbitMqConn {
        return createConnection {}
    }

    fun createConnection(afterClosed: () -> Unit): RabbitMqConn {
        val container = TestContainers.getRabbitMq()
        val factory = ConnectionFactory()
        factory.setUri(container.getConnectionString())
        val nnConnection = RabbitMqConn(factory)
        EcosTestExecutionListener.doWhenExecutionFinished { _, _ ->
            nnConnection.close()
            afterClosed.invoke()
        }
        return nnConnection
    }

    fun getConnection(): RabbitMqConn {
        val connection = this.connection
        if (connection == null) {
            val container = TestContainers.getRabbitMq()
            val factory = ConnectionFactory()
            factory.setUri(container.getConnectionString())
            val nnConnection = createConnection { this.connection = null }
            this.connection = nnConnection
            return nnConnection
        }
        return connection
    }
}
