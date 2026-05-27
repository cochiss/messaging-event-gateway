package com.smg.meg.schema

import com.fasterxml.jackson.databind.ObjectMapper
import com.smg.meg.model.document.Topic
import com.smg.meg.model.document.TopicSchemaValidation
import com.smg.meg.repository.TopicRepository
import com.smg.meg.repository.TopicSchemaValidationRepository
import com.smg.meg.service.TopicSchemaValidationService
import com.smg.meg.service.TopicService
import org.springframework.amqp.rabbit.core.RabbitAdmin
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import java.util.Optional

class TopicSchemaValidationServiceTest {

    private val topicRepository: TopicRepository = mock()
    private val schemaRepository: TopicSchemaValidationRepository = mock()
    private val validator = TopicSchemaValidator(ObjectMapper())
    private val topicService = TopicService(topicRepository, mock<RabbitAdmin>(), 65536)
    private val service = TopicSchemaValidationService(topicService, topicRepository, schemaRepository, validator)

    @Test
    fun `validate payload passes with valid monto cbu du`() {
        whenever(schemaRepository.findByTopicIdAndTopicVersionAndEnabledTrue("reintegros", 1))
            .thenReturn(configuredSchema("reintegros", 1))

        val payload = mapOf(
            "monto" to 250,
            "cbu" to "2312312312331234567890",
            "du" to "29345928"
        )

        assertDoesNotThrow {
            service.validatePayloadIfConfigured("reintegros", 1, payload)
        }
    }

    @Test
    fun `validate payload rejects monto non numeric or not positive`() {
        whenever(schemaRepository.findByTopicIdAndTopicVersionAndEnabledTrue("reintegros", 1))
            .thenReturn(configuredSchema("reintegros", 1))

        val payload = mapOf(
            "monto" to -10,
            "cbu" to "2312312312331234567890",
            "du" to "29345928"
        )

        val ex = assertThrows(ResponseStatusException::class.java) {
            service.validatePayloadIfConfigured("reintegros", 1, payload)
        }
        assertEquals(HttpStatus.BAD_REQUEST, ex.statusCode)
        assertTrue(ex.reason?.contains("does not match schema") == true)
    }

    @Test
    fun `validate payload rejects cbu length different from 22`() {
        whenever(schemaRepository.findByTopicIdAndTopicVersionAndEnabledTrue("reintegros", 1))
            .thenReturn(configuredSchema("reintegros", 1))

        val payload = mapOf(
            "monto" to 250,
            "cbu" to "12345",
            "du" to "29345928"
        )

        val ex = assertThrows(ResponseStatusException::class.java) {
            service.validatePayloadIfConfigured("reintegros", 1, payload)
        }
        assertEquals(HttpStatus.BAD_REQUEST, ex.statusCode)
    }

    @Test
    fun `validate payload rejects du when not numeric or out of length`() {
        whenever(schemaRepository.findByTopicIdAndTopicVersionAndEnabledTrue("reintegros", 1))
            .thenReturn(configuredSchema("reintegros", 1))

        val payload = mapOf(
            "monto" to 250,
            "cbu" to "2312312312331234567890",
            "du" to "12A"
        )

        val ex = assertThrows(ResponseStatusException::class.java) {
            service.validatePayloadIfConfigured("reintegros", 1, payload)
        }
        assertEquals(HttpStatus.BAD_REQUEST, ex.statusCode)
    }

    @Test
    fun `create schema validation rejects unknown topic version`() {
        whenever(topicRepository.findById("reintegros")).thenReturn(
            Optional.of(
                Topic(
                    id = "reintegros",
                    version = 2,
                    description = "Topic",
                    ownerApp = "test",
                    publishToken = "topic-token-1",
                    maxBodyBytes = 65536,
                    rabbitExchange = "ex.reintegros"
                )
            )
        )

        val ex = assertThrows(ResponseStatusException::class.java) {
            service.create("reintegros", 1, true, "schema", sampleSchema(), "topic-token-1")
        }
        assertEquals(HttpStatus.BAD_REQUEST, ex.statusCode)
    }

    @Test
    fun `create schema validation rejects invalid topic token`() {
        whenever(topicRepository.findById("reintegros")).thenReturn(
            Optional.of(
                Topic(
                    id = "reintegros",
                    version = 1,
                    description = "Topic",
                    ownerApp = "test",
                    publishToken = "topic-token-1",
                    maxBodyBytes = 65536,
                    rabbitExchange = "ex.reintegros"
                )
            )
        )

        val ex = assertThrows(ResponseStatusException::class.java) {
            service.create("reintegros", 1, true, "schema", sampleSchema(), "wrong-token")
        }
        assertEquals(HttpStatus.FORBIDDEN, ex.statusCode)
    }

    @Test
    fun `update schema validation rejects invalid topic token`() {
        whenever(topicRepository.findById("reintegros")).thenReturn(
            Optional.of(
                Topic(
                    id = "reintegros",
                    version = 1,
                    description = "Topic",
                    ownerApp = "test",
                    publishToken = "topic-token-1",
                    maxBodyBytes = 65536,
                    rabbitExchange = "ex.reintegros"
                )
            )
        )
        whenever(schemaRepository.findByTopicIdAndTopicVersion("reintegros", 1))
            .thenReturn(configuredSchema("reintegros", 1))

        val ex = assertThrows(ResponseStatusException::class.java) {
            service.update("reintegros", 1, true, "schema", sampleSchema(), "wrong-token")
        }
        assertEquals(HttpStatus.FORBIDDEN, ex.statusCode)
    }

    private fun configuredSchema(topicId: String, topicVersion: Int) =
        TopicSchemaValidation(
            id = "$topicId:v$topicVersion",
            topicId = topicId,
            topicVersion = topicVersion,
            enabled = true,
            description = "schema",
            schema = sampleSchema()
        )

    private fun sampleSchema(): Map<String, Any> =
        mapOf(
            "\$schema" to "http://json-schema.org/draft-07/schema#",
            "type" to "object",
            "required" to listOf("monto", "cbu", "du"),
            "properties" to mapOf(
                "monto" to mapOf(
                    "type" to "number",
                    "exclusiveMinimum" to 0
                ),
                "cbu" to mapOf(
                    "type" to "string",
                    "minLength" to 22,
                    "maxLength" to 22
                ),
                "du" to mapOf(
                    "type" to "string",
                    "minLength" to 7,
                    "maxLength" to 8,
                    "pattern" to "^[0-9]+$"
                )
            ),
            "additionalProperties" to true
        )
}

