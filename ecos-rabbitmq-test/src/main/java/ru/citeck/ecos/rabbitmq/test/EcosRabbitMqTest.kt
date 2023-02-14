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
    private var containerByConn = Collections.synchronizedMap(IdentityHashMap<RabbitMqConn, RabbitMqContainer>())
    private val mainContainerReserved = AtomicBoolean()

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
        val container = TestContainers.getRabbitMq(key)
        val factory = ConnectionFactory()
        factory.setUri(container.getConnectionString())
        val nnConnection = RabbitMqConn(factory)
        containerByConn[nnConnection] = container
        val wasClosed = AtomicBoolean(false)
        val closeImpl = {
            if (wasClosed.compareAndSet(false, true)) {
                beforeClosed.invoke()
                nnConnection.close()
                container.release()
                containerByConn.remove(nnConnection)
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
    fun getContainer(connection: RabbitMqConn): RabbitMqContainer? {
        return containerByConn[connection]
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
            if (key == "" && mainContainerReserved.compareAndSet(false, true)) {
                val container = containerByConn[nnConnection]!!
                container.reserve()
                EcosTestExecutionListener.doWhenTestPlanExecutionFinished { container.release() }
            }
            this.connection[connKey] = nnConnection
            return nnConnection
        }
        return connection
    }
}