package com.smg.meg.service

import com.smg.meg.controller.MessageRequest
import com.smg.meg.model.document.AuditHeader
import com.smg.meg.model.document.MessageAudit
import com.smg.meg.repository.MessageAuditRepository
import com.smg.meg.repository.TopicRepository
import com.smg.meg.worker.MessagePublisher
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.dao.DuplicateKeyException
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import java.util.Locale
import java.util.UUID

@Service
class MessageService(
    private val topicRepository: TopicRepository,
    private val messageAuditRepository: MessageAuditRepository,
    private val messagePublisher: MessagePublisher,
    private val objectMapper: ObjectMapper,
    private val topicSchemaValidationService: TopicSchemaValidationService
) {
    fun publish(
        topicId: String,
        topicVersion: Int,
        request: MessageRequest,
        idempotencyKey: String = "svc-${UUID.randomUUID()}",
        topicToken: String = "topic-token-not-provided",
        correlationId: String = "corr-${UUID.randomUUID()}",
        sourceApp: String = "internal-service"
    ): Map<String, String> {
        val normalizedTopicId = topicId.trim().lowercase(Locale.ROOT)
        val topic = topicRepository.findById(normalizedTopicId).orElseThrow {
            throw ResponseStatusException(HttpStatus.NOT_FOUND, "Topic '$normalizedTopicId' not found")
        }
        if (topic.version != topicVersion) {
            throw ResponseStatusException(
                HttpStatus.BAD_REQUEST,
                "Topic '$normalizedTopicId' current version is ${topic.version}, publish requested for version $topicVersion"
            )
        }
        if (topic.publishToken.isBlank() || topic.publishToken != topicToken.trim()) {
            throw ResponseStatusException(
                HttpStatus.FORBIDDEN,
                "Invalid topic token for topic '$normalizedTopicId'"
            )
        }
        val payloadSizeBytes = objectMapper.writeValueAsBytes(request.payload).size
        if (payloadSizeBytes > topic.maxBodyBytes) {
            throw ResponseStatusException(
                HttpStatus.PAYLOAD_TOO_LARGE,
                "payload size $payloadSizeBytes exceeds maxBodyBytes ${topic.maxBodyBytes} for topic $normalizedTopicId"
            )
        }
        topicSchemaValidationService.validatePayloadIfConfigured(normalizedTopicId, topicVersion, request.payload)

        val normalizedKey = idempotencyKey.trim()
        if (normalizedKey.isEmpty()) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Idempotency-Key must not be blank")
        }
        val existing = messageAuditRepository.findByTopicIdAndTopicVersionAndIdempotencyKey(
            normalizedTopicId,
            topicVersion,
            normalizedKey
        )
        if (existing != null) {
            return mapOf(
                "messageId" to existing.id,
                "status" to "ENQUEUED",
                "idempotencyKey" to normalizedKey,
                "deduplicated" to "true"
            )
        }

        val messageId = messagePublisher.sendToExchange(normalizedTopicId, request, correlationId, sourceApp)
        try {
            messageAuditRepository.save(
                MessageAudit(
                    id = messageId,
                    topicId = normalizedTopicId,
                    topicVersion = topicVersion,
                    idempotencyKey = normalizedKey,
                    header = AuditHeader(
                        userCreated = request.user,
                        correlationId = correlationId,
                        sourceApp = sourceApp,
                        eventType = request.eventType,
                        eventVersion = request.eventVersion
                    ),
                    payload = request.payload
                )
            )
        } catch (ex: DuplicateKeyException) {
            val stored = messageAuditRepository.findByTopicIdAndTopicVersionAndIdempotencyKey(
                normalizedTopicId,
                topicVersion,
                normalizedKey
            ) ?: throw ResponseStatusException(
                HttpStatus.CONFLICT,
                "Idempotency key conflict for topic '$normalizedTopicId' version $topicVersion"
            )
            return mapOf(
                "messageId" to stored.id,
                "status" to "ENQUEUED",
                "idempotencyKey" to normalizedKey,
                "deduplicated" to "true"
            )
        }

        val response = mutableMapOf(
            "messageId" to messageId,
            "status" to "ENQUEUED"
        )
        response["idempotencyKey"] = normalizedKey
        response["deduplicated"] = "false"
        return response
    }
}
