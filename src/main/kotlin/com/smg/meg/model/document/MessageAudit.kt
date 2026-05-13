package com.smg.meg.model.document

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

@Document("messages_audit")
data class MessageAudit(
    @Id
    val id: String,
    val topicId: String,
    val topicVersion: Int,
    val idempotencyKey: String? = null,
    val header: AuditHeader,
    val payload: Any
)

data class AuditHeader(
    val userCreated: String,
    val correlationId: String? = null,
    val sourceApp: String? = null,
    val eventType: String? = null,
    val eventVersion: Int? = null,
    val createdAt: Instant = Instant.now()
)
