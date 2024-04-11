package ru.citeck.ecos.rabbitmq

import com.rabbitmq.client.ConnectionFactory
import mu.KotlinLogging
import ru.citeck.ecos.commons.x509.EcosX509Registry
import ru.citeck.ecos.commons.x509.EmptyX509Registry
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.webapp.api.task.EcosTasksApi
import java.security.KeyStore
import javax.net.ssl.*

class RabbitMqConnFactory {

    companion object {
        val log = KotlinLogging.logger {}
    }

    private lateinit var tasksApi: EcosTasksApi
    private var micrometerContext: EcosMicrometerContext = EcosMicrometerContext.NOOP
    private var x509Registry: EcosX509Registry = EmptyX509Registry

    @JvmOverloads
    fun init(
        tasksApi: EcosTasksApi,
        micrometerContext: EcosMicrometerContext = EcosMicrometerContext.NOOP,
        x509Registry: EcosX509Registry = EmptyX509Registry
    ) {
        this.tasksApi = tasksApi
        this.micrometerContext = micrometerContext
        this.x509Registry = x509Registry
    }

    @JvmOverloads
    fun createConnection(
        props: RabbitMqConnProps,
        initDelayMs: Long = 10_000
    ): RabbitMqConn? {

        val host = props.host

        if (host.isBlank()) {
            log.error { "RabbitMq host is empty. Props: $props" }
            return null
        }

        val connectionFactory = ConnectionFactory()
        connectionFactory.isAutomaticRecoveryEnabled = true
        connectionFactory.host = props.host
        connectionFactory.username = props.username
        connectionFactory.password = props.password
        connectionFactory.virtualHost = props.virtualHost

        val port = props.port
        if (port != null) {
            connectionFactory.port = port
        }

        val tlsProps = props.tls
        if (tlsProps?.enabled == true) {

            var keyManagers: Array<KeyManager>? = null
            var trustManagers: Array<TrustManager>? = null

            val clientKeyName = tlsProps.clientKey ?: ""
            if (clientKeyName.isNotBlank()) {

                val clientKeyCert = x509Registry.getKeyWithCert(clientKeyName)

                val keyStore = KeyStore.getInstance(KeyStore.getDefaultType())
                keyStore.load(null, null)
                keyStore.setKeyEntry("key", clientKeyCert.key, "".toCharArray(), arrayOf(clientKeyCert.cert))

                val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
                kmf.init(keyStore, "".toCharArray())
                keyManagers = kmf.keyManagers
            }

            val trustedCertsNames = tlsProps.trustedCerts ?: ""
            if (trustedCertsNames.isNotBlank()) {

                val trustedCerts = x509Registry.getCertificates(trustedCertsNames)

                val trustStore = KeyStore.getInstance(KeyStore.getDefaultType())
                trustStore.load(null, null)
                for (idx in trustedCerts.indices) {
                    trustStore.setCertificateEntry(idx.toString(), trustedCerts[idx])
                }
                val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                tmf.init(trustStore)
                trustManagers = tmf.trustManagers
            }

            val protocol = (tlsProps.protocol ?: "").ifBlank {
                ConnectionFactory.computeDefaultTlsProcotol(SSLContext.getDefault().supportedSSLParameters.protocols)
            }

            val context: SSLContext = SSLContext.getInstance(protocol)
            context.init(keyManagers, trustManagers, null)

            connectionFactory.useSslProtocol(context)
            if (tlsProps.verifyHostname != false) {
                connectionFactory.enableHostnameVerification()
            }
        }

        val executor = if (props.executor.isNotBlank()) {
            tasksApi.getExecutor(props.executor).getAsJavaExecutor()
        } else {
            null
        }

        return RabbitMqConn(connectionFactory, props, executor, initDelayMs, micrometerContext)
    }
}
