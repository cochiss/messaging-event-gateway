package com.smg.meg.repository

import com.smg.meg.model.document.Subscription
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository
import java.util.*

@Repository
interface SubscriptionRepository : MongoRepository<Subscription, String> {
    
    // Para buscar por el ID que definimos en el documento (ej: reintegros-auditoria-0000001)
    fun findByTopicId(topicId: String): List<Subscription>

    // Para validar el token de seguridad X-Sub-Token en consumos PULL
    fun findByIdAndToken(id: String, token: String): Optional<Subscription>

    fun findByTypeAndStatus(type: String, status: String): List<Subscription>
    fun findByTypeAndStatusIn(type: String, statuses: Collection<String>): List<Subscription>
}