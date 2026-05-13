package com.smg.meg.worker

import io.github.resilience4j.retry.RetryRegistry
import kotlinx.coroutines.*
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component
import org.springframework.web.client.RestTemplate

@Component
class PushEngineWorker(
    private val retryRegistry: RetryRegistry
) {
    private val restTemplate = RestTemplate()
    private val workerScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    // Se desactiva por defecto hasta completar la registración dinámica de colas PUSH.
    @RabbitListener(queues = ["q.placeholder"], autoStartup = "false")
    fun processPushMessage(message: Map<String, Any>, @Header("amqp_consumerQueue") queueName: String) {
        
        // Lanzamos una corrutina por cada mensaje para no bloquear el listener de Rabbit
        workerScope.launch {
            try {
                // Buscamos la configuración de reintentos
                val retry = retryRegistry.retry("pushBackend")
                
                retry.executeSupplier {
                    // Aquí iría la lógica para obtener la URL de la suscripción desde Mongo
                    // Por ahora simulamos el envío PUSH
                    val targetUrl = "http://api-cliente/webhook" 
                    
                    restTemplate.postForEntity(targetUrl, message, String::class.java)
                }
            } catch (e: Exception) {
                // Si agota reintentos, el mensaje irá a la DLQ automáticamente 
                // gracias a la configuración de la Queue en SubscriptionService
                throw e 
            }
        }
    }
}