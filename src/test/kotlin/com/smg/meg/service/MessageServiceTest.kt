package com.smg.meg.service

import com.smg.meg.controller.MessageRequest
import com.smg.meg.model.document.AuditHeader
import com.smg.meg.model.document.MessageAudit
import com.smg.meg.model.document.Topic
import com.smg.meg.repository.MessageAuditRepository
import com.smg.meg.repository.TopicRepository
import com.smg.meg.worker.MessagePublisher
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.Test
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.doAnswer
import org.springframework.http.HttpStatus
import org.springframework.dao.DuplicateKeyException
import org.springframework.web.server.ResponseStatusException
import java.util.Optional

@ExtendWith(MockitoExtension::class)
class MessageServiceTest {

    @Mock
    private lateinit var topicRepository: TopicRepository

    @Mock
    private lateinit var messageAuditRepository: MessageAuditRepository

    @Mock
    private lateinit var messagePublisher: MessagePublisher

    @Mock
    private lateinit var objectMapper: ObjectMapper

    @Mock
    private lateinit var topicSchemaValidationService: TopicSchemaValidationService

    @InjectMocks
    private lateinit var messageService: MessageService

    @Test
    fun `publish uses idempotency key to deduplicate`() {
        val topicId = "reintegros"
        val topicVersion = 1
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.of(topic(topicId, topicVersion)))
        whenever(objectMapper.writeValueAsBytes(any())).thenReturn(ByteArray(100))
        whenever(
            messageAuditRepository.findByTopicIdAndTopicVersionAndIdempotencyKey(
                topicId,
                topicVersion,
                "abc-123"
            )
        ).thenReturn(
            MessageAudit(
                id = "msg_existing",
                topicId = topicId,
                topicVersion = topicVersion,
                idempotencyKey = "abc-123",
                header = AuditHeader(userCreated = "jdoe"),
                payload = mapOf("monto" to 100)
            )
        )

        val result = messageService.publish(
            topicId,
            topicVersion,
            MessageRequest("jdoe", "reintegro.solicitado", 1, mapOf("monto" to 100)),
            "abc-123",
            "topic-token-1",
            "corr-1",
            "finanzas-api"
        )

        assertEquals("msg_existing", result["messageId"])
        assertEquals("true", result["deduplicated"])
        verify(messagePublisher, never()).sendToExchange(any(), any(), any(), any())
    }

    @Test
    fun `publish creates message when idempotency key is new`() {
        val topicId = "reintegros"
        val topicVersion = 1
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.of(topic(topicId, topicVersion)))
        whenever(objectMapper.writeValueAsBytes(any())).thenReturn(ByteArray(100))
        whenever(
            messageAuditRepository.findByTopicIdAndTopicVersionAndIdempotencyKey(
                topicId,
                topicVersion,
                "abc-123"
            )
        ).thenReturn(null)
        whenever(messagePublisher.sendToExchange(eq(topicId), any(), any(), any())).thenReturn("msg_new")
        whenever(messageAuditRepository.save(Mockito.any(MessageAudit::class.java))).thenAnswer { it.arguments[0] }

        val result = messageService.publish(
            topicId,
            topicVersion,
            MessageRequest("jdoe", "reintegro.solicitado", 1, mapOf("monto" to 100)),
            "abc-123",
            "topic-token-1",
            "corr-1",
            "finanzas-api"
        )

        assertEquals("msg_new", result["messageId"])
        assertEquals("false", result["deduplicated"])
        verify(messagePublisher, times(1)).sendToExchange(eq(topicId), any(), any(), any())
        verify(messageAuditRepository, times(1)).save(Mockito.any(MessageAudit::class.java))
    }

    @Test
    fun `publish rejects mismatched topic version`() {
        val topicId = "reintegros"
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.of(topic(topicId, 2)))

        val ex = assertThrows(ResponseStatusException::class.java) {
            messageService.publish(topicId, 1, MessageRequest("jdoe", "reintegro.solicitado", 1, mapOf("monto" to 100)), "abc-123", "topic-token-1", "corr-1", "finanzas-api")
        }

        assertEquals(HttpStatus.BAD_REQUEST, ex.statusCode)
    }

    @Test
    fun `publish rejects blank idempotency key`() {
        val topicId = "reintegros"
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.of(topic(topicId, 1)))
        whenever(objectMapper.writeValueAsBytes(any())).thenReturn(ByteArray(100))

        val ex = assertThrows(ResponseStatusException::class.java) {
            messageService.publish(topicId, 1, MessageRequest("jdoe", "reintegro.solicitado", 1, mapOf("monto" to 100)), "   ", "topic-token-1", "corr-1", "finanzas-api")
        }

        assertEquals(HttpStatus.BAD_REQUEST, ex.statusCode)
        assertTrue(ex.reason?.contains("Idempotency-Key") == true)
    }

    @Test
    fun `publish rejects invalid topic token`() {
        val topicId = "reintegros"
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.of(topic(topicId, 1)))

        val ex = assertThrows(ResponseStatusException::class.java) {
            messageService.publish(
                topicId,
                1,
                MessageRequest("jdoe", "reintegro.solicitado", 1, mapOf("monto" to 100)),
                "abc-123",
                "wrong-topic-token",
                "corr-1",
                "finanzas-api"
            )
        }

        assertEquals(HttpStatus.FORBIDDEN, ex.statusCode)
        assertTrue(ex.reason?.contains("Invalid topic token") == true)
    }

    @Test
    fun `publish rejects payload larger than topic maxBodyBytes`() {
        val topicId = "reintegros"
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.of(topic(topicId, 1).copy(maxBodyBytes = 10)))
        whenever(objectMapper.writeValueAsBytes(any())).thenReturn(ByteArray(11))

        val ex = assertThrows(ResponseStatusException::class.java) {
            messageService.publish(topicId, 1, MessageRequest("jdoe", "reintegro.solicitado", 1, mapOf("monto" to 100)), "abc-123", "topic-token-1", "corr-1", "finanzas-api")
        }

        assertEquals(HttpStatus.PAYLOAD_TOO_LARGE, ex.statusCode)
        assertTrue(ex.reason?.contains("exceeds maxBodyBytes") == true)
    }

    @Test
    fun `publish rejects payload that does not match topic schema`() {
        val topicId = "reintegros"
        val payload = mapOf("monto" to "not-number")
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.of(topic(topicId, 1)))
        whenever(objectMapper.writeValueAsBytes(any())).thenReturn(ByteArray(20))
        doThrow(ResponseStatusException(HttpStatus.BAD_REQUEST, "Payload does not match schema"))
            .`when`(topicSchemaValidationService)
            .validatePayloadIfConfigured(topicId, 1, payload)

        val ex = assertThrows(ResponseStatusException::class.java) {
            messageService.publish(topicId, 1, MessageRequest("jdoe", "reintegro.solicitado", 1, payload), "abc-123", "topic-token-1", "corr-1", "finanzas-api")
        }

        assertEquals(HttpStatus.BAD_REQUEST, ex.statusCode)
        assertTrue(ex.reason?.contains("does not match schema") == true)
        verify(messagePublisher, never()).sendToExchange(any(), any(), any(), any())
    }

    @Test
    fun `publish returns deduplicated when unique index detects concurrent duplicate`() {
        val topicId = "reintegros"
        val topicVersion = 1
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.of(topic(topicId, topicVersion)))
        whenever(objectMapper.writeValueAsBytes(any())).thenReturn(ByteArray(20))
        whenever(
            messageAuditRepository.findByTopicIdAndTopicVersionAndIdempotencyKey(
                topicId,
                topicVersion,
                "abc-123"
            )
        ).thenReturn(null).thenReturn(
            MessageAudit(
                id = "msg_existing",
                topicId = topicId,
                topicVersion = topicVersion,
                idempotencyKey = "abc-123",
                header = AuditHeader(userCreated = "jdoe"),
                payload = mapOf("monto" to 100)
            )
        )
        whenever(messagePublisher.sendToExchange(eq(topicId), any(), any(), any())).thenReturn("msg_new")
        doAnswer { throw DuplicateKeyException("dup") }.whenever(messageAuditRepository).save(Mockito.any(MessageAudit::class.java))

        val result = messageService.publish(
            topicId,
            topicVersion,
            MessageRequest("jdoe", "reintegro.solicitado", 1, mapOf("monto" to 100)),
            "abc-123",
            "topic-token-1",
            "corr-1",
            "finanzas-api"
        )

        assertEquals("msg_existing", result["messageId"])
        assertEquals("true", result["deduplicated"])
    }

    @Test
    fun `publish returns conflict when duplicate key occurs and stored message is missing`() {
        val topicId = "reintegros"
        val topicVersion = 1
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.of(topic(topicId, topicVersion)))
        whenever(objectMapper.writeValueAsBytes(any())).thenReturn(ByteArray(20))
        whenever(
            messageAuditRepository.findByTopicIdAndTopicVersionAndIdempotencyKey(
                topicId,
                topicVersion,
                "abc-123"
            )
        ).thenReturn(null)
        whenever(messagePublisher.sendToExchange(eq(topicId), any(), any(), any())).thenReturn("msg_new")
        doAnswer { throw DuplicateKeyException("dup") }.whenever(messageAuditRepository).save(Mockito.any(MessageAudit::class.java))

        val ex = assertThrows(ResponseStatusException::class.java) {
            messageService.publish(
                topicId,
                topicVersion,
                MessageRequest("jdoe", "reintegro.solicitado", 1, mapOf("monto" to 100)),
                "abc-123",
                "topic-token-1",
                "corr-1",
                "finanzas-api"
            )
        }

        assertEquals(HttpStatus.CONFLICT, ex.statusCode)
        assertTrue(ex.reason?.contains("Idempotency key conflict") == true)
    }

    private fun topic(id: String, version: Int) =
        Topic(
            id = id,
            version = version,
            description = "Topic",
            ownerApp = "test",
            publishToken = "topic-token-1",
            maxBodyBytes = 65536,
            rabbitExchange = "ex.$id"
        )
}

