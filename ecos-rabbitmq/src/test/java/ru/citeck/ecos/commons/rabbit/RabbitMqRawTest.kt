package ru.citeck.ecos.commons.rabbit

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Delivery
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.rabbitmq.RabbitMqConn

class RabbitMqRawTest {

    @Test
    fun test() {

        val factory: ConnectionFactory = MockConnectionFactory()
        val connection = RabbitMqConn(factory)

        connection.waitUntilReady(5_000)

        val bodies = mutableListOf<ByteArray>()

        connection.doWithNewChannel { ch ->
            ch.declareQueue("test-queue", true)
            ch.addAckedConsumer("test-queue", Delivery::class.java) { msg, headers ->
                bodies.add(msg.getContent().body)
            }
        }

        val txt = "some text"
        connection.doWithNewChannel { ch ->
            ch.publishMsg("test-queue", txt.toByteArray())
        }

        Thread.sleep(100)

        assertThat(bodies[0]).isEqualTo(txt.toByteArray())
    }
}
