package ru.citeck.ecos.rabbitmq.test

import com.rabbitmq.client.ConnectionFactory
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.test.commons.containers.container.rabbitmq.RabbitMqContainer
import ru.citeck.ecos.test.commons.listener.EcosTestExecutionListener
import java.util.IdentityHashMap
import java.util.concurrent.atomic.AtomicBoolean

object EcosRabbitMqTest {

    private var connection = IdentityHashMap<Thread, RabbitMqConn>()

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
        nnConnection.waitUntilReady(100_000)
        return nnConnection
    }

    fun getContainer(): RabbitMqContainer {
        return TestContainers.getRabbitMq()
    }

    fun getConnection(): RabbitMqConn {
        val thread = Thread.currentThread()
        val connection = this.connection[thread]
        if (connection == null) {
            val nnConnection = createConnection { this.connection.remove(thread) }
            this.connection[thread] = nnConnection
            return nnConnection
        }
        return connection
    }
}
