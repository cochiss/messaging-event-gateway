package com.smg.meg.model.document

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

@Document("subscription_pending_messages")
data class PendingMessage(
    @Id
    val id: String,
    val subscriptionId: String,
    val messageId: String,
    val bucket: String, // MAIN | DLQ
    val payload: Any,
    val createdAt: Instant = Instant.now(),
    val visibilityUntil: Instant? = null,
    val deliveryCount: Int = 0
)
