package com.smg.meg.service

import com.smg.meg.model.document.TopicSchemaValidation
import com.smg.meg.repository.TopicRepository
import com.smg.meg.repository.TopicSchemaValidationRepository
import com.smg.meg.schema.TopicSchemaValidator
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.server.ResponseStatusException
import java.util.Locale

@Service
class TopicSchemaValidationService(
    private val topicRepository: TopicRepository,
    private val repository: TopicSchemaValidationRepository,
    private val topicSchemaValidator: TopicSchemaValidator
) {

    @Transactional
    fun create(topicId: String, topicVersion: Int, enabled: Boolean, description: String?, schema: Map<String, Any>): TopicSchemaValidation {
        val normalizedTopicId = topicId.trim().lowercase(Locale.ROOT)
        requireExistingTopicVersion(normalizedTopicId, topicVersion)
        if (repository.findByTopicIdAndTopicVersion(normalizedTopicId, topicVersion) != null) {
            throw ResponseStatusException(
                HttpStatus.CONFLICT,
                "Schema validation already exists for topic '$normalizedTopicId' version $topicVersion"
            )
        }
        topicSchemaValidator.validateSchemaDefinition(schema)
        return repository.save(
            TopicSchemaValidation(
                id = "$normalizedTopicId:v$topicVersion",
                topicId = normalizedTopicId,
                topicVersion = topicVersion,
                enabled = enabled,
                description = description,
                schema = schema
            )
        )
    }

    @Transactional
    fun update(topicId: String, topicVersion: Int, enabled: Boolean, description: String?, schema: Map<String, Any>): TopicSchemaValidation {
        val normalizedTopicId = topicId.trim().lowercase(Locale.ROOT)
        requireExistingTopicVersion(normalizedTopicId, topicVersion)
        val existing = repository.findByTopicIdAndTopicVersion(normalizedTopicId, topicVersion)
            ?: throw ResponseStatusException(
                HttpStatus.NOT_FOUND,
                "Schema validation not found for topic '$normalizedTopicId' version $topicVersion"
            )
        topicSchemaValidator.validateSchemaDefinition(schema)
        return repository.save(
            existing.copy(
                enabled = enabled,
                description = description,
                schema = schema
            )
        )
    }

    fun get(topicId: String, topicVersion: Int): TopicSchemaValidation =
        repository.findByTopicIdAndTopicVersion(topicId.trim().lowercase(Locale.ROOT), topicVersion)
            ?: throw ResponseStatusException(
                HttpStatus.NOT_FOUND,
                "Schema validation not found for topic '$topicId' version $topicVersion"
            )

    fun validatePayloadIfConfigured(topicId: String, topicVersion: Int, payload: Any) {
        val configured = repository.findByTopicIdAndTopicVersionAndEnabledTrue(topicId, topicVersion) ?: return
        topicSchemaValidator.validatePayloadOrThrow(topicId, configured.schema, payload)
    }

    private fun requireExistingTopicVersion(topicId: String, topicVersion: Int) {
        val topic = topicRepository.findById(topicId).orElseThrow {
            ResponseStatusException(HttpStatus.NOT_FOUND, "Topic '$topicId' not found")
        }
        if (topic.version != topicVersion) {
            throw ResponseStatusException(
                HttpStatus.BAD_REQUEST,
                "Topic '$topicId' current version is ${topic.version}, requested schema version $topicVersion"
            )
        }
    }
}
