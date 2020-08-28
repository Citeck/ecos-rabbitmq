package ru.citeck.ecos.commons.rabbit

interface EcosRabbitConnectionProvider {

    fun getConnection() : EcosRabbitConnection?
}
