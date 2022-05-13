package ru.citeck.ecos.rabbitmq

class RabbitMqConnProps(
    val host: String = "localhost",
    val username: String = "admin",
    val password: String = "admin",
    val virtualHost: String = "/",
    val port: Int? = null
)
