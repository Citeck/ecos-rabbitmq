package ru.citeck.ecos.commons.rabbit

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.rabbitmq.RabbitMqUtils

class RabbitMqUtilsTest {

    @Test
    fun test() {
        val protocol = RabbitMqUtils.computeDefaultTlsProtocol()
        assertThat(protocol).isNotBlank
    }
}
