package ru.citeck.ecos.rabbitmq.ds

import ru.citeck.ecos.webapp.api.datasource.EcosDataSource

interface RabbitMqDataSource : EcosDataSource {

    fun getConnection(): RabbitMqConnection
}
