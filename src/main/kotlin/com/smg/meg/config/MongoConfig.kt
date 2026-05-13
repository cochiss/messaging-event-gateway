package com.smg.meg.config

import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.config.EnableMongoAuditing
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.index.Index
import org.springframework.data.mongodb.core.index.IndexOperations
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.domain.Sort
import java.time.Duration
import jakarta.annotation.PostConstruct

@Configuration
@EnableMongoAuditing // Habilita que se completen automáticamente fechas de creación
class MongoConfig(private val mongoTemplate: MongoTemplate) {

    @Value("\${spring.data.mongodb.ttl:2592000}")
    private val ttlSeconds: Long = 2592000 // Valor por defecto: 30 días

    @PostConstruct
    fun initIndices() {
        // Configuración del TTL Index para la auditoría de mensajes [cite: 10]
        val indexOps: IndexOperations = mongoTemplate.indexOps("messages_audit")
        
        val ttlIndex = Index()
            .on("header.createdAt", Sort.Direction.ASC)
            .expire(Duration.ofSeconds(ttlSeconds))
        
        indexOps.ensureIndex(ttlIndex)
        indexOps.ensureIndex(
            Index()
                .on("topicId", Sort.Direction.ASC)
                .on("topicVersion", Sort.Direction.ASC)
                .on("idempotencyKey", Sort.Direction.ASC)
                .unique()
                .named("ux_messages_audit_topic_version_idempotency")
        )

        val pendingOps: IndexOperations = mongoTemplate.indexOps("subscription_pending_messages")
        // Optimiza busqueda de pendientes por sub + bucket + orden de llegada
        pendingOps.ensureIndex(
            Index()
                .on("subscriptionId", Sort.Direction.ASC)
                .on("bucket", Sort.Direction.ASC)
                .on("createdAt", Sort.Direction.ASC)
                .named("idx_pending_sub_bucket_created")
        )
        // Optimiza filtro de visibilidad para pulls recurrentes
        pendingOps.ensureIndex(
            Index()
                .on("subscriptionId", Sort.Direction.ASC)
                .on("bucket", Sort.Direction.ASC)
                .on("visibilityUntil", Sort.Direction.ASC)
                .named("idx_pending_sub_bucket_visibility")
        )
    }
}