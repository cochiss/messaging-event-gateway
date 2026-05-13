package com.smg.meg.repository

import com.smg.meg.model.document.ConfResilience
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository
import java.util.Optional

@Repository
interface ConfResilienceRepository : MongoRepository<ConfResilience, String> {
    fun findBySubscriptionId(subscriptionId: String): Optional<ConfResilience>
}

