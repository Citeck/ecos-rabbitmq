package ru.citeck.ecos.commons.rabbit

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.rabbitmq.RabbitMqConnCtx
import java.util.*
import kotlin.concurrent.thread

class RabbitMqConnCtxTest {

    @Test
    fun test() {

        val threads = 100
        val iterationsInThread = 100
        val ctx = RabbitMqConnCtx()
        val dtoResults =
            Collections.synchronizedList(mutableListOf<DtoToSerialize>())
        (0 until threads).map {
            thread {
                for (i in 0 until iterationsInThread) {
                    val dto = DtoToSerialize()
                    val dtoRes = ctx.fromMsgBodyBytes(
                        ctx.toMsgBodyBytes(dto),
                        DtoToSerialize::class.java
                    )!!
                    dtoResults.add(dtoRes)
                }
            }
        }.forEach { it.join() }

        assertThat(dtoResults).hasSize(threads * iterationsInThread)
        assertThat(dtoResults).allMatch { it == DtoToSerialize() }
    }

    data class DtoToSerialize(
        val strField: String = "Abc",
        val intField: Int = 123,
        val longField: Long = 123L,
        val boolField: Boolean = true,
        val data: ObjectData = ObjectData.create()
            .set("first", "second")
            .set("third", "fourth")
    )
}
