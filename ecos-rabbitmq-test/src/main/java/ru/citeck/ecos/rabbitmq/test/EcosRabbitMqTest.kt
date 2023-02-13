package ru.citeck.ecos.rabbitmq.test

import com.rabbitmq.client.ConnectionFactory
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.test.commons.containers.container.rabbitmq.RabbitMqContainer
import ru.citeck.ecos.test.commons.listener.EcosTestExecutionListener
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

object EcosRabbitMqTest {

    private var connection = Collections.synchronizedMap(LinkedHashMap<Pair<Any, Thread>, RabbitMqConn>())

    @JvmStatic
    fun createConnection(): RabbitMqConn {
        return createConnection("", true) {}
    }

    @JvmStatic
    @JvmOverloads
    fun createConnection(key: Any, closeAfterTest: Boolean = true): RabbitMqConn {
        return createConnection(key, closeAfterTest) {}
    }

    @JvmStatic
    @JvmOverloads
    fun createConnection(closeAfterTest: Boolean = true, beforeClosed: () -> Unit): RabbitMqConn {
        return createConnection("", closeAfterTest, beforeClosed)
    }

    @JvmStatic
    @JvmOverloads
    fun createConnection(key: Any, closeAfterTest: Boolean = true, beforeClosed: () -> Unit): RabbitMqConn {
        val container = getContainer(key)
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
        if (closeAfterTest) {
            EcosTestExecutionListener.doWhenExecutionFinished { _, _ -> closeImpl() }
        }
        container.doBeforeStop(closeImpl)
        nnConnection.waitUntilReady(100_000)
        return nnConnection
    }

    @JvmStatic
    @JvmOverloads
    fun getContainer(key: Any = ""): RabbitMqContainer {
        return TestContainers.getRabbitMq(key)
    }

    @JvmStatic
    @JvmOverloads
    @Synchronized
    fun getConnection(key: Any = ""): RabbitMqConn {
        val thread = Thread.currentThread()
        val connKey = key to thread
        val connection = this.connection[connKey]
        if (connection == null) {
            val nnConnection = createConnection(true) { this.connection.remove(connKey) }
            this.connection[connKey] = nnConnection
            return nnConnection
        }
        return connection
    }
}
