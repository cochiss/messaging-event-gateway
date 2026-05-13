package com.smg.meg.service

import com.smg.meg.model.document.Topic
import com.smg.meg.repository.TopicRepository
import org.springframework.amqp.core.TopicExchange
import org.springframework.amqp.rabbit.core.RabbitAdmin
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.server.ResponseStatusException
import java.util.Locale
import java.util.UUID

@Service
class TopicService(
    private val repository: TopicRepository,
    private val rabbitAdmin: RabbitAdmin,
    @Value("\${meg.topic.max-body-bytes:65536}")
    private val defaultMaxBodyBytes: Int
) {

    fun listTopics(page: Int, size: Int): List<Topic> {
        val safePage = page.coerceAtLeast(0)
        val safeSize = size.coerceAtLeast(1)
        return repository.findAll().drop(safePage * safeSize).take(safeSize)
    }

    @Transactional
    fun createTopic(
        id: String,
        version: Int,
        description: String,
        ownerApp: String,
        maxBodyBytes: Int?
    ): Topic {
        val normalizedId = id.trim().lowercase(Locale.ROOT)
        if (version < 1) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Topic version must be >= 1")
        }
        val resolvedMaxBodyBytes = (maxBodyBytes ?: defaultMaxBodyBytes).coerceAtLeast(1)

        val existing = repository.findById(normalizedId).orElse(null)
        if (existing == null && version != 1) {
            throw ResponseStatusException(
                HttpStatus.BAD_REQUEST,
                "Cannot create topic '$normalizedId' at version $version without existing version ${version - 1}"
            )
        }
        if (existing != null && version <= existing.version) {
            throw ResponseStatusException(
                HttpStatus.CONFLICT,
                "Topic '$normalizedId' version $version already exists or is older than current version ${existing.version}"
            )
        }
        if (existing != null && version != existing.version + 1) {
            throw ResponseStatusException(
                HttpStatus.BAD_REQUEST,
                "Topic '$normalizedId' version must advance sequentially. Current=${existing.version}, requested=$version"
            )
        }

        val exchangeName = "ex.$normalizedId"
        
        // 1. Crear el Exchange en RabbitMQ de forma dinámica
        val exchange = TopicExchange(exchangeName, true, false)
        rabbitAdmin.declareExchange(exchange)
        
        // 2. Guardar la definición en MongoDB
        val topic = Topic(
            id = normalizedId,
            version = version,
            description = description,
            ownerApp = ownerApp,
            publishToken = UUID.randomUUID().toString(),
            maxBodyBytes = resolvedMaxBodyBytes,
            rabbitExchange = exchangeName
        )
        return repository.save(topic)
    }

    @Transactional
    fun updateTopicConfig(id: String, description: String, ownerApp: String, maxBodyBytes: Int): Topic {
        val normalizedId = id.trim().lowercase(Locale.ROOT)
        val current = repository.findById(normalizedId).orElseThrow {
            ResponseStatusException(HttpStatus.NOT_FOUND, "Topic '$normalizedId' not found")
        }
        if (maxBodyBytes < 1) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "maxBodyBytes must be >= 1")
        }
        val updated = current.copy(
            description = description,
            ownerApp = ownerApp,
            maxBodyBytes = maxBodyBytes
        )
        return repository.save(updated)
    }
}