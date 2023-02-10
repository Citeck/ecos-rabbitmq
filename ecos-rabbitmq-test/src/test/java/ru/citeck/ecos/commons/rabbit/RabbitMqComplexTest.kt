package ru.citeck.ecos.commons.rabbit

import org.junit.jupiter.api.Test
import ru.citeck.ecos.rabbitmq.test.EcosRabbitMqTest
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class RabbitMqComplexTest {

    @Test
    fun test() {

        val conn1 = EcosRabbitMqTest.createConnection()
        val conn2 = EcosRabbitMqTest.createConnection()

        val messages = Collections.synchronizedList(mutableListOf<String>())
        val completed = AtomicBoolean()
        val messagesCount = 30
        val receivedCount = AtomicInteger()
        val results = Collections.synchronizedList(mutableListOf<String>())

        repeat(2) {
            conn1.doWithNewChannel { channel ->
                channel.declareQueue("queue-1", false)
                channel.declareQueue("queue-2", false)
                channel.addAckedConsumer("queue-1", String::class.java) { msg, _ ->
                    messages.add("before queue-1: " + msg.getContent())
                    channel.publishMsg("queue-2", msg.getContent())
                    while (!results.contains(msg.getContent())) {
                        Thread.sleep(1000)
                    }
                    messages.add("after queue-1: " + msg.getContent())
                }
            }
        }
        repeat(2) {
            conn1.doWithNewChannel { channel ->
                channel.declareQueue("queue-1-res", false)
                channel.addAckedConsumer("queue-1-res", String::class.java) { msg, _ ->
                    messages.add("queue-1-res: " + msg.getContent())
                    results.add(msg.getContent())
                    if (receivedCount.incrementAndGet() == messagesCount) {
                        completed.set(true)
                    }
                    if (receivedCount.get() % 10 == 0) {
                        println("${Instant.now()} Received count: ${receivedCount.get()}")
                    }
                }
            }
        }

        repeat(2) {
            conn2.doWithNewChannel { channel ->
                channel.declareQueue("queue-1", false)
                channel.declareQueue("queue-2", false)
                channel.addAckedConsumer("queue-2", String::class.java) { msg, _ ->
                    messages.add("before queue-2: " + msg.getContent())
                    Thread.sleep(100)
                    channel.publishMsg("queue-1-res", msg.getContent())
                    messages.add("after queue-2: " + msg.getContent())
                }
            }
        }

        Thread.sleep(1000)

        val conn3 = EcosRabbitMqTest.createConnection()
        conn3.doWithNewChannel { channel ->
            for (i in 0 until messagesCount) {
                channel.publishMsg("queue-1", "abc-$i")
            }
        }

        while (!completed.get()) {
            Thread.sleep(1000)
        }
        println(messages)
    }
}
