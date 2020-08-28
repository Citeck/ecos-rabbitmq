package ru.citeck.ecos.rabbitmq

interface EcosRabbitConnectionProvider {

    fun getConnection() : EcosRabbitConnection?
}
