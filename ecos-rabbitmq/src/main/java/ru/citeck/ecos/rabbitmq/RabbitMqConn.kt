package ru.citeck.ecos.rabbitmq

import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import mu.KotlinLogging
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer
import kotlin.concurrent.thread
import kotlin.math.min

class RabbitMqConn @JvmOverloads constructor(
    private val connectionFactory: ConnectionFactory,
    executor: ExecutorService? = null,
    private val initSleepMs: Long = 0L
) {

    companion object {
        val log = KotlinLogging.logger {}
    }

    @Volatile
    private var initStarted = false

    private var connection: Connection? = null
    private val initFuture = CompletableFuture<Boolean>()
    private val postInitActions = ConcurrentLinkedQueue<Consumer<Connection>>()
    private val connectionContext = RabbitMqConnCtx()

    private var wasClosed = AtomicBoolean(false)

    private val executor = executor ?: Executors.newFixedThreadPool(16)

    private val initializerEnabled = AtomicBoolean(true)
    private val shutdownHook = Thread {
        initializerEnabled.set(false)
        if (!wasClosed.get()) {
            if (this.connection?.isOpen == true) {
                this.connection?.close()
            }
            wasClosed.set(true)
        }
    }

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

        val connectionFailuresCount = AtomicInteger(0)

        Runtime.getRuntime().addShutdownHook(shutdownHook)

        thread(start = true, isDaemon = false, name = "ECOS rabbit connection initializer") {

            if (initSleepMs > 0) {
                log.info { "Rabbit initialization will be started after ${initSleepMs / 1000.0} sec." }
            }

            Thread.sleep(initSleepMs)

            var tryWithoutLogErrorStartTime = System.currentTimeMillis()

            while (initializerEnabled.get()) {

                var connection: Connection? = null

                try {
                    connection = connectionFactory.newConnection(executor)

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
                    connectionFailuresCount.incrementAndGet()
                    try {
                        this.connection = null
                        if (connection?.isOpen == true) {
                            connection.close()
                        }
                    } catch (e: Exception) {
                        log.error(e) { "Error while connection closing" }
                    }
                    val msg = "Cannot configure connection to RabbitMQ"
                    if (System.currentTimeMillis() - tryWithoutLogErrorStartTime > 120_000) {
                        tryWithoutLogErrorStartTime = System.currentTimeMillis()
                        log.error(e) { msg }
                    } else {
                        var mostSpecificMsg = e.message
                        var ex: Throwable
                        var cause: Throwable? = e
                        while (cause != null) {
                            ex = cause
                            if (!cause.message.isNullOrBlank()) {
                                mostSpecificMsg = cause.message
                            }
                            cause = ex.cause
                        }
                        log.error("$msg: '$mostSpecificMsg'")
                    }
                    Thread.sleep(
                        min(
                            2000L * (connectionFailuresCount.get() + 1),
                            20_000L
                        )
                    )
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
                channel.basicQos(qos, true)
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
        if (!wasClosed.get()) {
            if (connection?.isOpen == true) {
                connection?.close()
            }
            Runtime.getRuntime().removeShutdownHook(shutdownHook)
            wasClosed.set(true)
        }
    }

    fun isClosed(): Boolean {
        return wasClosed.get()
    }
}
