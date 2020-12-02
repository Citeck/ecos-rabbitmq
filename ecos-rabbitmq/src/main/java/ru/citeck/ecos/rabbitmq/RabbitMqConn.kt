package ru.citeck.ecos.rabbitmq

import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import mu.KotlinLogging
import java.lang.IllegalStateException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
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

    private fun init() {

        if (initStarted) {
            return
        }

        thread(start = true, isDaemon = false, name = "ECOS rabbit connection initializer") {

            if (initSleepMs > 0) {
                log.info { "Rabbit initialization will be started after ${initSleepMs / 1000.0} sec." }
            }

            Thread.sleep(initSleepMs)

            var tryWithoutLogErrorStartTime = System.currentTimeMillis()

            while (true) {

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
        initStarted = true
    }

    fun waitUntilReady(timeoutMs: Long) {
        init()
        initFuture.get(timeoutMs, TimeUnit.MILLISECONDS)
    }

    fun doWithNewChannel(action: Consumer<RabbitMqChannel>) {
        doWithConnection(
            Consumer {
                action.accept(RabbitMqChannel(it.createChannel(), connectionContext))
            }
        )
    }

    fun doWithConnection(action: Consumer<Connection>) {
        val connection = this.connection
        if (connection != null) {
            action.accept(connection)
        } else {
            init()
            postInitActions.add(action)
        }
    }

    fun close() {
        connection?.close()
    }
}
