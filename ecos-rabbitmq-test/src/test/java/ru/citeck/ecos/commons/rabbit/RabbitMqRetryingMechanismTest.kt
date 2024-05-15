package ru.citeck.ecos.commons.rabbit

import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import ru.citeck.ecos.rabbitmq.RabbitMqChannel
import ru.citeck.ecos.rabbitmq.RabbitMqChannel.Companion.DLQ_POSTFIX
import ru.citeck.ecos.rabbitmq.RabbitMqConn
import ru.citeck.ecos.rabbitmq.test.EcosRabbitMqTest
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class RabbitMqRetryingMechanismTest {

    private val atMostSec = 5L

    private lateinit var connection: RabbitMqConn

    @BeforeEach
    fun setUo() {
        connection = EcosRabbitMqTest.createConnection()
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    fun `check repeat count of retrying mechanism`() {
        val queueName = "queue-retry-check-count"
        val retryCount = 5
        val retryDelay = 100L
        val actualRetry = AtomicInteger(0)

        connection.doWithNewChannel { channel ->
            channel.declareQueuesWithRetrying(queueName, retryDelay)
            channel.addConsumerWithRetrying(queueName, String::class.java, retryCount) { _, _ ->
                actualRetry.incrementAndGet()
                error("Error")
            }
        }

        connection.doWithNewChannel {
            it.publishMsg(queueName, "test")
        }

        Awaitility.await().atMost(atMostSec, TimeUnit.SECONDS).untilAsserted {
            val expectedProcessCount = retryCount + 1
            assertThat(actualRetry.get()).isEqualTo(expectedProcessCount)
        }
    }

    @Test
    fun `check repeat delay of retrying mechanism`() {
        val queueName = "queue-retry-check-delay"
        val retryCount = 1
        val retryDelay = 100L

        val now = Instant.now()
        lateinit var lastProcessTime: Instant

        connection.doWithNewChannel { channel ->
            channel.declareQueuesWithRetrying(queueName, retryDelay)
            channel.addConsumerWithRetrying(queueName, String::class.java, retryCount) { _, _ ->
                lastProcessTime = Instant.now()
                error("Error")
            }
        }

        connection.doWithNewChannel {
            it.publishMsg(queueName, "test")
        }

        Awaitility.await().atMost(atMostSec, TimeUnit.SECONDS).untilAsserted {
            val actualDelay = AtomicLong(lastProcessTime.toEpochMilli() - now.toEpochMilli())
            assertThat(actualDelay.get()).isGreaterThanOrEqualTo(retryDelay)
        }
    }

    @Test
    fun `check repeat count and delay of retrying mechanism`() {
        val queueName = "queue-retry-check-combination-count-delay"
        val retryCount = 5
        val retryDelay = 100L
        val actualRetry = AtomicInteger(0)
        val now = Instant.now()
        lateinit var lastProcessTime: Instant

        connection.doWithNewChannel { channel ->
            channel.declareQueuesWithRetrying(queueName, retryDelay)
            channel.addConsumerWithRetrying(queueName, String::class.java, retryCount) { _, _ ->
                actualRetry.incrementAndGet()
                lastProcessTime = Instant.now()
                error("Error")
            }
        }

        connection.doWithNewChannel {
            it.publishMsg(queueName, "test")
        }

        Awaitility.await().atMost(atMostSec, TimeUnit.SECONDS).untilAsserted {
            val expectedProcessCount = retryCount + 1
            assertThat(actualRetry.get()).isEqualTo(expectedProcessCount)

            val actualDelay = AtomicLong(lastProcessTime.toEpochMilli() - now.toEpochMilli())
            assertThat(actualDelay.get()).isGreaterThanOrEqualTo(retryDelay * retryCount)
        }
    }

    @Test
    fun `check message is sent to dead letter queue after exceeding max retry count`() {
        val queueName = "queue-retry-check-dlq"
        val retryCount = 3
        val retryDelay = 100L
        val dlqMessage = AtomicBoolean(false)

        connection.doWithNewChannel { channel ->
            channel.declareQueuesWithRetrying(queueName, retryDelay)
            channel.addConsumerWithRetrying(queueName, String::class.java, retryCount) { _, _ ->
                error("Error")
            }
            channel.addAckedConsumer("$queueName$DLQ_POSTFIX", String::class.java) { msg, _ ->
                dlqMessage.set(true)
                msg.ack()
            }
        }

        connection.doWithNewChannel {
            it.publishMsg(queueName, "test")
        }

        Awaitility.await().atMost(atMostSec, TimeUnit.SECONDS).untilAsserted {
            assertThat(dlqMessage.get()).isTrue()
        }
    }

    @Test
    fun `check message is not sent to dead letter if processed successfully`() {
        val queueName = "queue-retry-check-success-dead-letter-queue"
        val retryCount = 3
        val retryDelay = 100L
        val dlqMessage = AtomicBoolean(false)

        connection.doWithNewChannel { channel ->
            channel.declareQueuesWithRetrying(queueName, retryDelay)
            channel.addConsumerWithRetrying(queueName, String::class.java, retryCount) { _, _ ->
            }
            channel.addAckedConsumer("$queueName$DLQ_POSTFIX", String::class.java) { msg, _ ->
                dlqMessage.set(true)
                msg.ack()
            }
        }

        connection.doWithNewChannel {
            it.publishMsg(queueName, "test")
        }

        Awaitility.await().atMost(atMostSec, TimeUnit.SECONDS).untilAsserted {
            assertThat(dlqMessage.get()).isFalse()
        }
    }

    @Test
    fun `check message is processed successfully without retries when there are no errors`() {
        val queueName = "queue-retry-check-success"
        val retryCount = 3
        val retryDelay = 100L
        val actualRetry = AtomicInteger(0)

        connection.doWithNewChannel { channel ->
            channel.declareQueuesWithRetrying(queueName, retryDelay)
            channel.addConsumerWithRetrying(queueName, String::class.java, retryCount) { _, _ ->
                actualRetry.incrementAndGet()
            }
        }

        connection.doWithNewChannel {
            it.publishMsg(queueName, "test")
        }

        Awaitility.await().atMost(atMostSec, TimeUnit.SECONDS).untilAsserted {
            assertThat(actualRetry.get()).isEqualTo(1)
        }
    }

    @Test
    fun `check retrying mechanism when processing is successful after a few retries`() {
        val queueName = "queue-retry-check-success-after-retries"
        val retryCount = 5
        val retryDelay = 100L
        val actualRetry = AtomicInteger(0)
        val success = AtomicBoolean(false)

        connection.doWithNewChannel { channel ->
            channel.declareQueuesWithRetrying(queueName, retryDelay)
            channel.addConsumerWithRetrying(queueName, String::class.java, retryCount) { _, _ ->
                if (actualRetry.incrementAndGet() < 2) {
                    error("Error")
                } else {
                    success.set(true)
                }
            }
        }

        connection.doWithNewChannel {
            it.publishMsg(queueName, "test")
        }

        Awaitility.await().atMost(atMostSec, TimeUnit.SECONDS).untilAsserted {
            assertThat(actualRetry.get()).isEqualTo(2)
            assertThat(success.get()).isTrue()
        }
    }

    @Test
    fun `check message stays in queue if no consumers registered`() {
        val queueName = "queue-no-consumers"
        val retryDelay = 100L

        lateinit var testChannel: RabbitMqChannel

        connection.doWithNewChannel { channel ->
            testChannel = channel
            testChannel.declareQueuesWithRetrying(queueName, retryDelay)
        }

        connection.doWithNewChannel {
            it.publishMsg(queueName, "test")
        }

        Awaitility.await().atMost(atMostSec, TimeUnit.SECONDS).untilAsserted {
            val queueDeclareOk = testChannel.getOriginalChannel().queueDeclarePassive(queueName)
            assertThat(queueDeclareOk.messageCount).isEqualTo(1)
        }
    }
}
