package ru.citeck.ecos.rabbitmq.spring

import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

@Configuration
@ComponentScan(basePackages = ["ru.citeck.ecos.rabbitmq.spring"])
open class EcosRabbitMqAutoConfiguration
