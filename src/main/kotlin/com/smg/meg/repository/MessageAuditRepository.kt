package com.smg.meg.repository

import com.smg.meg.model.document.MessageAudit
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository

@Repository
interface MessageAuditRepository : MongoRepository<MessageAudit, String> {
    fun findByTopicIdAndTopicVersionAndIdempotencyKey(
        topicId: String,
        topicVersion: Int,
        idempotencyKey: String
    ): MessageAudit?
}