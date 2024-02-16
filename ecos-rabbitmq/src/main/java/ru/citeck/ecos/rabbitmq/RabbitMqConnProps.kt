package ru.citeck.ecos.rabbitmq

class RabbitMqConnProps(
    val host: String = "localhost",
    val username: String = "admin",
    val password: String = "admin",
    val virtualHost: String = "/",
    val port: Int? = null,
    val tls: Tls? = null
) {

    class Tls(
        val enabled: Boolean = false,
        val clientKey: String? = null,
        val trustedCerts: String? = null,
        /**
         * SSL protocol to use. By default, configured by the Rabbit client library.
         */
        val protocol: String? = null,
        /**
         * Whether to enable hostname verification. Requires AMQP client 4.8 or above and
         * defaults to true when a suitable client version is used.
         */
        var verifyHostname: Boolean? = null
    )
}
