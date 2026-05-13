package com.smg.meg.model.document

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document

@Document("topic_schema_validation")
data class TopicSchemaValidation(
    @Id
    val id: String,
    val topicId: String,
    val topicVersion: Int,
    val enabled: Boolean = true,
    val description: String? = null,
    val schema: Map<String, Any>
)
