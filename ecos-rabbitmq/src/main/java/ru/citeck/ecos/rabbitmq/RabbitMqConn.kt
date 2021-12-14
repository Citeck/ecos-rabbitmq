package ru.citeck.ecos.rabbitmq

import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer
import kotlin.concurrent.thread

class RabbitMqConn @JvmOverloads constructor(
    private val connectionFactory: ConnectionFactory,
    private val initSleepMs: Long = 0L,
    private val threads: Int = 16
) {

    companion object {
        val log = KotlinLogging.logger {}
    }

    @Volatile
    private var initStarted = false

    private var connection: Connection? = null
    private val initFuture = CompletableFuture<Boolean>()
    private val postInitActions = ConcurrentLinkedQueue<Consumer<Connection>>()
    private val connectionContext = RabbitMqConnCtx(this)

    @Synchronized
    private fun init() {
        if (initStarted) {
            return
        }
        initStarted = true
        try {
            initThreadImpl()
        } catch (e: Exception) {
            initStarted = false
            throw e
        }
    }

    private fun initThreadImpl() {

        val initializerEnabled = AtomicBoolean(true)

        Runtime.getRuntime().addShutdownHook(
            Thread {
                log.info("Shutdown hook triggered")
                initializerEnabled.set(false)
                this.connection?.close()
                log.info("Shutdown hook completed")
            }
        )

        thread(start = true, isDaemon = false, name = "ECOS rabbit connection initializer") {

            if (initSleepMs > 0) {
                log.info { "Rabbit initialization will be started after ${initSleepMs / 1000.0} sec." }
            }

            Thread.sleep(initSleepMs)

            var tryWithoutLogErrorStartTime = System.currentTimeMillis()

            while (initializerEnabled.get()) {

                var connection: Connection? = null

                try {
                    connection = connectionFactory.newConnection(Executors.newFixedThreadPool(threads))

                    if (!connection.isOpen) {
                        throw IllegalStateException("Connection is not open")
                    }
                    val props = connection.serverProperties

                    log.info {
                        "Connected to ${props["product"]} " +
                            "version ${props["version"]} " +
                            "platform ${props["platform"]} " +
                            "information ${props["information"]}"
                    }

                    this.connection = connection

                    var action = postInitActions.poll()
                    while (action != null) {
                        action.accept(connection)
                        action = postInitActions.poll()
                    }

                    initFuture.complete(true)

                    break
                } catch (e: Exception) {
                    try {
                        this.connection = null
                        connection?.close()
                    } catch (e: Exception) {
                        log.error(e) { "Error while connection closing" }
                    }
                    val msg = "Cannot configure connection to RabbitMQ"
                    if (System.currentTimeMillis() - tryWithoutLogErrorStartTime > 120_000) {
                        tryWithoutLogErrorStartTime = System.currentTimeMillis()
                        log.error(e) { msg }
                    } else {
                        var ex: Throwable
                        var cause: Throwable? = e
                        while (cause != null) {
                            ex = cause
                            cause = ex.cause
                        }
                        log.error(msg + ": '" + e.message + "'")
                    }
                    Thread.sleep(20_000)
                }
            }
        }
    }

    fun waitUntilReady(timeoutMs: Long) {
        init()
        initFuture.get(timeoutMs, TimeUnit.MILLISECONDS)
    }

    fun doWithNewChannel(action: Consumer<RabbitMqChannel>) {
        doWithNewChannel(1, action)
    }

    fun doWithNewChannel(qos: Int, action: Consumer<RabbitMqChannel>) {
        doWithConnection(
            Consumer {
                val channel = it.createChannel()
                channel.basicQos(qos)
                action.accept(RabbitMqChannel(channel, connectionContext))
            }
        )
    }

    fun doWithConnection(action: Consumer<Connection>) {
        val connection = this.connection
        if (connection != null) {
            action.accept(connection)
        } else {
            postInitActions.add(action)
            init()
        }
    }

    fun close() {
        connection?.close()
    }
}
