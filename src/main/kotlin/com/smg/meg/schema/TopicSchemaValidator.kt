package com.smg.meg.schema

import com.fasterxml.jackson.databind.ObjectMapper
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.SpecVersion
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.server.ResponseStatusException

@Component
class TopicSchemaValidator(
    private val objectMapper: ObjectMapper
) {
    private val schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)

    fun validateSchemaDefinition(schema: Map<String, Any>) {
        runCatching {
            val schemaNode = objectMapper.valueToTree<com.fasterxml.jackson.databind.JsonNode>(schema)
            schemaFactory.getSchema(schemaNode)
        }.getOrElse { ex ->
            throw ResponseStatusException(
                HttpStatus.BAD_REQUEST,
                "Invalid JSON schema definition: ${ex.message}"
            )
        }
    }

    fun validatePayloadOrThrow(topicId: String, schema: Map<String, Any>, payload: Any) {
        val schemaNode = objectMapper.valueToTree<com.fasterxml.jackson.databind.JsonNode>(schema)
        val payloadNode = objectMapper.valueToTree<com.fasterxml.jackson.databind.JsonNode>(payload)
        val jsonSchema = schemaFactory.getSchema(schemaNode)
        val violations = jsonSchema.validate(payloadNode)
        if (violations.isNotEmpty()) {
            val detail = violations.first().message
            throw ResponseStatusException(
                HttpStatus.BAD_REQUEST,
                "Payload does not match schema for topic '$topicId': $detail"
            )
        }
    }
}
