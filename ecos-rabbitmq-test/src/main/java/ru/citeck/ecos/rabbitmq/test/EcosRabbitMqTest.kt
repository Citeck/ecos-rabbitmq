package ru.citeck.ecos.rabbitmq.test

import com.rabbitmq.client.ConnectionFactory
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.test.commons.containers.container.rabbitmq.RabbitMqContainer
import ru.citeck.ecos.test.commons.listener.EcosTestExecutionListener
import java.util.concurrent.atomic.AtomicBoolean

object EcosRabbitMqTest {

    private var connection = ThreadLocal<RabbitMqConn>()

    fun createConnection(): RabbitMqConn {
        return createConnection {}
    }

    fun createConnection(beforeClosed: () -> Unit): RabbitMqConn {
        val container = getContainer()
        val factory = ConnectionFactory()
        factory.setUri(container.getConnectionString())
        val nnConnection = RabbitMqConn(factory)
        val wasClosed = AtomicBoolean(false)
        val closeImpl = {
            if (wasClosed.compareAndSet(false, true)) {
                beforeClosed.invoke()
                nnConnection.close()
            }
        }
        EcosTestExecutionListener.doWhenExecutionFinished { _, _ -> closeImpl() }
        container.doBeforeStop(closeImpl)
        return nnConnection
    }

    fun getContainer(): RabbitMqContainer {
        return TestContainers.getRabbitMq()
    }

    fun getConnection(): RabbitMqConn {
        val connection = this.connection.get()
        if (connection == null) {
            val nnConnection = createConnection { this.connection.remove() }
            this.connection.set(nnConnection)
            return nnConnection
        }
        return connection
    }
}
