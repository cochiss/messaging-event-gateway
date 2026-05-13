package com.smg.meg.model.document

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document

@Document("topics")
data class Topic(
    @Id
    val id: String,
    val version: Int = 1,
    val description: String,
    val ownerApp: String,
    val publishToken: String = "",
    val maxBodyBytes: Int,
    val rabbitExchange: String,
    val status: String = "ACTIVE"
)
