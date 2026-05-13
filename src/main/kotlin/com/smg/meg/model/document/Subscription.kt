package com.smg.meg.model.document

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document

@Document("subscriptions")
data class Subscription(
    @Id
    val id: String,
    val topicId: String,
    val topicVersion: Int,
    val nameSub: String,
    val version: Int = 1,
    val description: String? = null,
    val type: String,
    val token: String,
    val urlRest: String? = null,
    val maxRetries: Int = 10,
    val maxDeliveryCountPull: Int = 10,
    val mainQueue: String,
    val dlq: String,
    val status: String = "ACTIVE"
)
