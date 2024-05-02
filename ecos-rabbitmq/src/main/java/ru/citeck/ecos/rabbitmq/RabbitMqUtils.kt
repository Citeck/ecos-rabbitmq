package ru.citeck.ecos.rabbitmq

import com.rabbitmq.client.ConnectionFactory
import javax.net.ssl.SSLContext

object RabbitMqUtils {

    private const val FALLBACK_TLS_PROTOCOL = "TLSv1.2"

    fun computeDefaultTlsProtocol(): String {
        return computeDefaultTlsProtocol(SSLContext.getDefault().supportedSSLParameters.protocols)
    }

    fun computeDefaultTlsProtocol(protocols: Array<String>): String {

        // this method renamed in rmq library and reflection used for compatibility
        val method = try {
            ConnectionFactory::class.java.getDeclaredMethod(
                "computeDefaultTlsProcotol",
                Array<String>::class.java
            )
        } catch (e: NoSuchMethodException) {
            try {
                ConnectionFactory::class.java.getDeclaredMethod(
                    "computeDefaultTlsProtocol",
                    Array<String>::class.java
                )
            } catch (e: NoSuchMethodException) {
                return FALLBACK_TLS_PROTOCOL
            }
        }
        return (method.invoke(null, protocols) as? String ?: "").ifBlank { FALLBACK_TLS_PROTOCOL }
    }
}
