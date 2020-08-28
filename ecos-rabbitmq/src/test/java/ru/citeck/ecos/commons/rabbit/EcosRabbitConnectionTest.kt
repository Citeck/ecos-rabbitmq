package ru.citeck.ecos.commons.rabbit

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.ConnectionFactory
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.utils.func.UncheckedBiConsumer
import ru.citeck.ecos.rabbitmq.EcosRabbitChannel
import ru.citeck.ecos.rabbitmq.EcosRabbitConnection
import java.util.function.Consumer
import kotlin.test.assertEquals

class EcosRabbitConnectionTest {

    @Test
    fun test() {

        val factory: ConnectionFactory = MockConnectionFactory()
        val connection = EcosRabbitConnection(factory)

        connection.waitUntilReady(5_000)

        val results = ArrayList<Message>()
        lateinit var channel: EcosRabbitChannel

        connection.doWithNewChannel(Consumer { newChannel -> channel = newChannel })

        channel.declareQueue("test", false)
        channel.addConsumer("test",
            Message::class.java,
            object : UncheckedBiConsumer<Message, Map<String, Any>> {

                override fun accept(arg0: Message, arg1: Map<String, Any>) {
                    results.add(arg0)
                }
            })

        val msg = Message(
            "field0",
            123,
            456L,
            Bytes(ByteArray(10) { it.toByte() }),
            Inner("inner field")
        )

        channel.publishMsg("test", msg)
        waitSize(results, 1)

        assertEquals(1, results.size)
        assertEquals(msg, results[0])

        channel.declareExchange("test-exchange", BuiltinExchangeType.TOPIC, true)
        channel.declareExchange("test-exchange", BuiltinExchangeType.TOPIC, true)

        channel.queueBind("test", "test-exchange", "routing.*")
        channel.publishMsg("test-exchange", "routing.inner", msg)

        waitSize(results, 2)
        assertEquals(2, results.size)
        assertEquals(msg, results[1])
    }

    private fun waitSize(collection: Collection<*>, expectedSize: Int) {
        var checkings = 10
        while (checkings > 0 && collection.size != expectedSize) {
            Thread.sleep(100)
            checkings--
        }
    }

    data class Message(
        val field0: String,
        val field1: Int,
        val field2: Long,
        val field3: Bytes,
        val inner: Inner
    )

    data class Inner(
        val innerField0: String
    )

    class Bytes (val bytes: ByteArray) {
        override fun equals(other: Any?): Boolean {
            if (this === other) {
                return true
            }
            if (javaClass != other?.javaClass) {
                return false
            }
            other as Bytes
            if (!bytes.contentEquals(other.bytes)) {
                return false
            }
            return true
        }

        override fun hashCode(): Int {
            return bytes.contentHashCode()
        }
    }
}
