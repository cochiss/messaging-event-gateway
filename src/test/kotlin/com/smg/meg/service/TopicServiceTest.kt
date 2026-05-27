package com.smg.meg.service

import com.smg.meg.model.document.Topic
import com.smg.meg.repository.TopicRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Captor
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.amqp.core.TopicExchange
import org.springframework.amqp.rabbit.core.RabbitAdmin
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import java.util.Optional

@ExtendWith(MockitoExtension::class)
class TopicServiceTest {

    @Mock
    private lateinit var topicRepository: TopicRepository

    @Mock
    private lateinit var rabbitAdmin: RabbitAdmin

    @Captor
    private lateinit var exchangeCaptor: ArgumentCaptor<TopicExchange>

    @Test
    fun `createTopic creates v1 topic when not exists`() {
        val topicId = "reintegros"
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.empty())
        whenever(topicRepository.save(any(Topic::class.java))).thenAnswer { it.getArgument<Topic>(0) }

        val service = TopicService(topicRepository, rabbitAdmin, 65536)
        val result = service.createTopic(topicId, 1, "Topic reintegros", "finanzas-api", null)

        verify(rabbitAdmin).declareExchange(exchangeCaptor.capture())
        assertEquals("ex.$topicId", exchangeCaptor.value.name)
        assertEquals(1, result.version)
        assertEquals(false, result.publishToken.isBlank())
        assertEquals(65536, result.maxBodyBytes)
    }

    @Test
    fun `createTopic rejects skipped version`() {
        val topicId = "reintegros"
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.empty())

        val ex = assertThrows(ResponseStatusException::class.java) {
            TopicService(topicRepository, rabbitAdmin, 65536)
                .createTopic(topicId, 3, "Topic reintegros", "finanzas-api", null)
        }

        assertEquals(HttpStatus.BAD_REQUEST, ex.statusCode)
        verify(topicRepository, never()).save(any(Topic::class.java))
    }

    @Test
    fun `createTopic rejects already existing version`() {
        val topicId = "reintegros"
        val existing = Topic(
            id = topicId,
            version = 2,
            description = "v2",
            ownerApp = "finanzas-api",
            maxBodyBytes = 65536,
            rabbitExchange = "ex.$topicId"
        )
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.of(existing))

        val ex = assertThrows(ResponseStatusException::class.java) {
            TopicService(topicRepository, rabbitAdmin, 65536)
                .createTopic(topicId, 2, "v2 again", "finanzas-api", null)
        }

        assertEquals(HttpStatus.CONFLICT, ex.statusCode)
    }

    @Test
    fun `createTopic accepts max body bytes override`() {
        val topicId = "reintegros"
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.empty())
        whenever(topicRepository.save(any(Topic::class.java))).thenAnswer { it.getArgument<Topic>(0) }

        val result = TopicService(topicRepository, rabbitAdmin, 65536)
            .createTopic(topicId, 1, "Topic reintegros", "finanzas-api", 1024)

        assertEquals(1024, result.maxBodyBytes)
    }

    @Test
    fun `createTopic normalizes id to lowercase`() {
        whenever(topicRepository.findById("solicitudes-reintegros")).thenReturn(Optional.empty())
        whenever(topicRepository.save(any(Topic::class.java))).thenAnswer { it.getArgument<Topic>(0) }

        val result = TopicService(topicRepository, rabbitAdmin, 65536)
            .createTopic("Solicitudes-Reintegros", 1, "Topic reintegros", "finanzas-api", null)

        assertEquals("solicitudes-reintegros", result.id)
        assertEquals("ex.solicitudes-reintegros", result.rabbitExchange)
    }

    @Test
    fun `updateTopicConfig updates only mutable fields`() {
        val topicId = "reintegros"
        val existing = Topic(
            id = topicId,
            version = 2,
            description = "old",
            ownerApp = "old-owner",
            publishToken = "topic-token-1",
            maxBodyBytes = 65536,
            rabbitExchange = "ex.$topicId"
        )
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.of(existing))
        whenever(topicRepository.save(any(Topic::class.java))).thenAnswer { it.getArgument<Topic>(0) }

        val updated = TopicService(topicRepository, rabbitAdmin, 65536)
            .updateTopicConfig(topicId, "new-desc", "new-owner", 2048, "topic-token-1")

        assertEquals(2, updated.version)
        assertEquals("new-desc", updated.description)
        assertEquals("new-owner", updated.ownerApp)
        assertEquals(2048, updated.maxBodyBytes)
        assertEquals("ex.$topicId", updated.rabbitExchange)
    }

    @Test
    fun `updateTopicConfig rejects invalid topic token`() {
        val topicId = "reintegros"
        val existing = Topic(
            id = topicId,
            version = 1,
            description = "old",
            ownerApp = "old-owner",
            publishToken = "topic-token-1",
            maxBodyBytes = 65536,
            rabbitExchange = "ex.$topicId"
        )
        whenever(topicRepository.findById(topicId)).thenReturn(Optional.of(existing))

        val ex = assertThrows(ResponseStatusException::class.java) {
            TopicService(topicRepository, rabbitAdmin, 65536)
                .updateTopicConfig(topicId, "new-desc", "new-owner", 2048, "wrong-token")
        }

        assertEquals(HttpStatus.FORBIDDEN, ex.statusCode)
        verify(topicRepository, never()).save(any(Topic::class.java))
    }
}
