package ru.citeck.ecos.commons.rabbit

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.rabbitmq.RabbitMqConnFactory
import ru.citeck.ecos.rabbitmq.RabbitMqConnProps
import ru.citeck.ecos.rabbitmq.ack.AckedMessage
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

// test should be executed with real rabbitmq to check QOS working
@Disabled
class RealRabbitMqTest {

    companion object {
        val QUEUES = listOf("test-queue-1", "test-queue-2")

        const val REAL_CONN_HOST = "localhost"
        const val REAL_CONN_USERNAME = "admin"
        const val REAL_CONN_PASSWORD = "admin"

        private const val MOCK_CONNECTION = false
    }

    @Test
    fun ackTest() {

        val connection = createRabbitMqConnection()
        val message = Message(12345)

        val queue = "ack-test-queue"

        val messagesCount = 1

        waitCallback<Unit> { callback ->

            connection.doWithNewChannel { channel ->
                channel.declareQueue(queue, true)
                repeat(messagesCount) {
                    channel.publishMsg(queue, message)
                }
                connection.doWithNewChannel { channel2 ->
                    channel2.addConsumer(queue, Message::class.java) { _, _ ->

                        callback.invoke(Unit)
                        error("Opps... (Expected)")
                    }
                }
            }
        }

        Thread.sleep(1000)

        val resultMessages = CopyOnWriteArrayList<Message>()
        connection.doWithNewChannel { channel ->
            channel.addConsumer(queue, Message::class.java) { msg, _ ->
                resultMessages.add(msg)
            }
        }

        Thread.sleep(3000)

        // not passed. Messages will be lost in channel2 with error(...)
        assertThat(resultMessages).hasSize(messagesCount)
    }

    @Test
    fun qosTest() {

        val messagesCount = 100
        val messageProcessingTime = 100L

        val ecosConn1 = createRabbitMqConnection()

        val channelsCount = 3
        val channelsMessages = Array(QUEUES.size) { Array(channelsCount) { CopyOnWriteArrayList<Message>() } }

        var messagesSent = false
        ecosConn1.doWithNewChannel { channel ->
            QUEUES.forEach { channel.declareQueue(it, false) }
            repeat(messagesCount) {
                QUEUES.forEach { queueName ->
                    channel.publishMsg(queueName, Message(messageProcessingTime))
                }
            }
            messagesSent = true
        }

        waitWhile { !messagesSent }

        val ecosConn2 = createRabbitMqConnection()

        val consumeMessage = { queueIdx: Int, channelIdx: Int, msg: Message ->
            if (msg.timeToProcess > 0) {
                Thread.sleep(msg.timeToProcess)
            }
            channelsMessages[queueIdx][channelIdx].add(msg)
        }
        val consumeAckMessage = { queueIdx: Int, channelIdx: Int, msg: AckedMessage<Message> ->
            consumeMessage(queueIdx, channelIdx, msg.getContent())
        }

        QUEUES.forEachIndexed { queueIndex, queueName ->
            repeat(channelsCount) { index ->
                ecosConn1.doWithNewChannel { channel ->
                    channel.addAckedConsumer(queueName, Message::class.java) { msg, _ ->
                        consumeAckMessage(queueIndex, index, msg)
                    }
                    if (index == 0 && queueIndex == 0) {
                        repeat(10) {
                            channel.addAckedConsumer(queueName, Message::class.java) { msg, _ ->
                                consumeAckMessage(queueIndex, index, msg)
                            }
                        }
                    }
                }
            }
        }

        waitWhile {
            channelsMessages.any { messages -> messages.sumOf { it.size } != messagesCount }
        }
        channelsMessages.forEachIndexed { idx, messages ->
            println("${QUEUES[idx]} messages by channels: " + messages.map { it.size })
        }

        ecosConn1.close()
        ecosConn2.close()
    }

    private fun waitWhile(condition: () -> Boolean) {
        val timeout = System.currentTimeMillis() + 15_000
        while (condition.invoke()) {
            if (System.currentTimeMillis() > timeout) {
                error("Timeout")
            }
            Thread.sleep(100)
        }
    }

    private fun <T> waitCallback(action: ((T) -> Unit) -> Unit): T {
        val result = AtomicReference<T>()
        val completedFlag = AtomicBoolean()
        action.invoke {
            result.set(it)
            completedFlag.set(true)
        }
        waitWhile { !completedFlag.get() }
        return result.get()
    }

    data class Message(
        val timeToProcess: Long = 0
    )

    private fun createRealRabbitMqConnection(): RabbitMqConn {

        val props = RabbitMqConnProps(
            host = REAL_CONN_HOST,
            username = REAL_CONN_USERNAME,
            password = REAL_CONN_PASSWORD
        )

        val rmqFactory = RabbitMqConnFactory()
        return rmqFactory.createConnection(props, initDelayMs = 0)!!
    }

    private fun createMockRabbitMqConnection(): RabbitMqConn {
        val factory = MockConnectionFactory()
        return RabbitMqConn(factory, 0L)
    }

    private fun createRabbitMqConnection(): RabbitMqConn {
        return if (MOCK_CONNECTION) {
            createMockRabbitMqConnection()
        } else {
            createRealRabbitMqConnection()
        }
    }
}
