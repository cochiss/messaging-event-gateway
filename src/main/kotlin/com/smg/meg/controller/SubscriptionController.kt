package com.smg.meg.controller

import com.smg.meg.model.document.Subscription
import com.smg.meg.model.document.ConfResilience
import com.smg.meg.service.ConfResilienceRequest
import com.smg.meg.service.SubscriptionService
import com.smg.meg.service.ConfResilienceService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.ExampleObject
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.parameters.RequestBody as SwaggerRequestBody
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.validation.Valid
import jakarta.validation.constraints.AssertTrue
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Size
import org.springframework.http.HttpStatus
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.*
import java.util.Locale

@RestController
@Validated
@Tag(name = "Subscriptions", description = "Gestion de suscripciones PULL/PUSH, ACK y DLQ")
class SubscriptionController(
    private val subscriptionService: SubscriptionService,
    private val confResilienceService: ConfResilienceService
) {

    @GetMapping("/subscriptions")
    @Operation(
        summary = "Listar suscripciones",
        description = "Retorna suscripciones paginadas, opcionalmente filtradas por topicId. Sin `token` (solo en POST create). Canales con nombres agnosticos (`deliveryChannel`, `deadLetterChannel`)."
    )
    @ApiResponses(
        ApiResponse(
            responseCode = "200",
            description = "Listado de suscripciones",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [
                        ExampleObject(
                            value = """[{"id":"topic-demo-v1-sub-procesador","topicId":"topic-demo","topicVersion":1,"nameSub":"sub-procesador","version":1,"description":"Procesador principal por PULL","type":"PULL","urlRest":null,"maxRetries":10,"maxDeliveryCountPull":10,"deliveryChannel":"q.topic-demo.v1.sub-procesador","deadLetterChannel":"q.topic-demo.v1.sub-procesador.dlq","status":"ACTIVE"}]"""
                        )
                    ]
                )
            ]
        )
    )
    fun listSubscriptions(
        @RequestParam(required = false)
        @Pattern(
            regexp = "^[A-Za-z]+(-[A-Za-z]+)*$",
            message = "topicId must contain only letters and hyphens"
        )
        @Size(max = 50, message = "topicId must not exceed 50 characters")
        topicId: String?,
        @RequestParam(defaultValue = "0") @Min(0) page: Int,
        @RequestParam(defaultValue = "10") @Min(1) size: Int
    ): List<SubscriptionResponse> =
        subscriptionService.listSubscriptions(topicId, page, size).map { SubscriptionResponse.from(it) }

    @PostMapping("/subscriptions")
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Crear suscripcion", description = "Crea una suscripcion PULL o PUSH para un topic/version")
    @ApiResponses(
        ApiResponse(
            responseCode = "201",
            description = "Suscripcion creada",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [
                        ExampleObject(
                            value = """{"id":"topic-demo-v1-sub-procesador","topicId":"topic-demo","topicVersion":1,"nameSub":"sub-procesador","version":1,"description":"Procesador principal por PULL","type":"PULL","urlRest":null,"maxRetries":10,"maxDeliveryCountPull":10,"deliveryChannel":"q.topic-demo.v1.sub-procesador","deadLetterChannel":"q.topic-demo.v1.sub-procesador.dlq","status":"ACTIVE","token":"6f838a72-97eb-45f4-9335-d931ea425e88"}"""
                        )
                    ]
                )
            ]
        ),
        ApiResponse(responseCode = "400", description = "Request invalido", content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"VALIDATION_ERROR","message":"urlRest is required for PUSH and must be null/blank for PULL"}""")])]),
        ApiResponse(responseCode = "404", description = "Topic no encontrado", content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"404 NOT_FOUND","message":"Topic 'topic-demo' not found"}""")])]),
        ApiResponse(responseCode = "409", description = "Suscripcion duplicada", content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"409 CONFLICT","message":"Subscription 'sub-procesador' already exists for topic 'topic-demo'"}""")])])
    )
    fun createSubscription(
        @SwaggerRequestBody(
            required = true,
            description = "Request de creacion de suscripcion",
            content = [
                Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = SubscriptionRequest::class),
                    examples = [
                        ExampleObject(
                            name = "pullSubscription",
                            value = """{"topicId":"topic-demo","topicVersion":1,"nameSub":"sub-procesador","description":"Procesador principal por PULL","type":"PULL","urlRest":null,"maxRetries":10,"maxDeliveryCountPull":10}"""
                        ),
                        ExampleObject(
                            name = "pushSubscription",
                            value = """{"topicId":"topic-demo","topicVersion":1,"nameSub":"sub-webhook","description":"Procesador principal por PUSH","type":"PUSH","urlRest":"http://example.com/webhooks/events","maxRetries":10,"maxDeliveryCountPull":10}"""
                        )
                    ]
                )
            ]
        )
        @RequestBody @Valid request: SubscriptionRequest
    ): SubscriptionCreatedResponse = SubscriptionCreatedResponse.from(
        subscriptionService.createSubscription(request.topicId, request)
    )

    @PutMapping("/subscriptions/{id}")
    @Operation(summary = "Actualizar suscripcion", description = "Actualiza descripcion, status (ACTIVE/INACTIVE), urlRest (solo PUSH) y maxDeliveryCountPull")
    @ApiResponses(
        ApiResponse(responseCode = "200", description = "Suscripcion actualizada"),
        ApiResponse(
            responseCode = "400",
            description = "Request invalido",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"400 BAD_REQUEST","message":"urlRest must be null/blank for PULL subscriptions"}""")])]
        ),
        ApiResponse(
            responseCode = "401",
            description = "Token de suscripcion invalido",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"401 UNAUTHORIZED","message":"Invalid subscription token"}""")])]
        )
    )
    fun updateSubscription(
        @PathVariable @NotBlank @Parameter(example = "topic-demo-v1-sub-procesador") id: String,
        @RequestHeader("X-Sub-Token") @NotBlank token: String,
        @SwaggerRequestBody(
            required = true,
            content = [
                Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = UpdateSubscriptionRequest::class),
                    examples = [ExampleObject(value = """{"description":"Procesador principal","status":"ACTIVE","urlRest":null,"maxDeliveryCountPull":10}""")]
                )
            ]
        )
        @RequestBody @Valid request: UpdateSubscriptionRequest
    ): SubscriptionResponse = SubscriptionResponse.from(
        subscriptionService.updateSubscription(id, token, request)
    )

    // Consumo de Mensajes (PULL) [cite: 66]
    @GetMapping("/subscriptions/{id}/messages")
    @Operation(summary = "Pull de mensajes", description = "Obtiene mensajes pendientes para una suscripcion PULL. Si un mensaje alcanza maxDeliveryCountPull se mueve automaticamente a DLQ con motivo max-delivery-exceeded. Si la suscripcion esta INACTIVE o PAUSED_BY_SYSTEM responde 403.")
    @ApiResponses(
        ApiResponse(
            responseCode = "200",
            description = "Mensajes obtenidos",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [
                        ExampleObject(
                            value = """[{"header":{"messageId":"msg_950d117f","correlationId":"corr-001","sourceApp":"finanzas-api","eventType":"solicitud.reintegro.creada","eventVersion":1},"user":"jdoe","payload":{"monto":200,"du":"29345928","cbu":"0110567620056701234560"}}]"""
                        )
                    ]
                )
            ]
        ),
        ApiResponse(
            responseCode = "403",
            description = "Suscripcion no operable (INACTIVE o PAUSED_BY_SYSTEM)",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [
                        ExampleObject(name = "inactive", value = """{"error":"403 FORBIDDEN","message":"Subscription 'topic-demo-v1-sub-procesador' is INACTIVE"}"""),
                        ExampleObject(name = "pausedBySystem", value = """{"error":"403 FORBIDDEN","message":"Subscription 'topic-demo-v1-sub-procesador' is PAUSED_BY_SYSTEM"}""")
                    ]
                )
            ]
        ),
        ApiResponse(
            responseCode = "401",
            description = "Token de suscripcion invalido",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"401 UNAUTHORIZED","message":"Invalid subscription token"}""")])]
        )
    )
    fun getMessages(
        @PathVariable @NotBlank id: String,
        @RequestParam(defaultValue = "0") @Min(0) page: Int,
        @RequestParam(defaultValue = "10") @Min(1) size: Int,
        @RequestParam(required = false) @Min(1) rows: Int?,
        @RequestHeader("X-Sub-Token") @NotBlank token: String
    ) = subscriptionService.pullMessages(id, page, rows ?: size, token)

    // Confirmar Procesamiento (ACK) [cite: 83, 84]
    @PutMapping("/subscriptions/{id}/messages/{messageId}")
    @Operation(summary = "ACK de mensaje", description = "Confirma PROCESSED o REJECT para un mensaje de una suscripcion")
    @ApiResponses(
        ApiResponse(responseCode = "200", description = "ACK aplicado"),
        ApiResponse(
            responseCode = "400",
            description = "Accion invalida",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"400 BAD_REQUEST","message":"Invalid action 'INVALID'"}""")])]
        ),
        ApiResponse(
            responseCode = "401",
            description = "Token de suscripcion invalido",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"401 UNAUTHORIZED","message":"Invalid subscription token"}""")])]
        ),
        ApiResponse(
            responseCode = "404",
            description = "Mensaje pending no encontrado",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"404 NOT_FOUND","message":"Message 'msg_950d117f' is not pending for subscription 'topic-demo-v1-sub-procesador'"}""")])]
        )
    )
    fun acknowledgeMessage(
        @PathVariable @NotBlank id: String,
        @PathVariable @NotBlank messageId: String,
        @RequestHeader("X-Sub-Token") @NotBlank token: String,
        @SwaggerRequestBody(
            required = true,
            content = [
                Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = AckRequest::class),
                    examples = [
                        ExampleObject(name = "processed", value = """{"action":"PROCESSED"}"""),
                        ExampleObject(name = "reject", value = """{"action":"REJECT"}""")
                    ]
                )
            ]
        )
        @RequestBody @Valid body: AckRequest
    ) = subscriptionService.processAck(id, messageId, token, body.action)

    // Reprocesar Mensaje de Dead Letter [cite: 114]
    @GetMapping("/subscriptions/{id}/dead-letters")
    @Operation(summary = "Listar DLQ", description = "Lista mensajes en dead letter queue de una suscripcion")
    @ApiResponses(
        ApiResponse(
            responseCode = "200",
            description = "Mensajes DLQ",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """[{"header":{"messageId":"msg_950d117f"},"payload":{"monto":200,"du":"29345928","cbu":"0110567620056701234560"}}]""")])]
        ),
        ApiResponse(
            responseCode = "401",
            description = "Token de suscripcion invalido",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"401 UNAUTHORIZED","message":"Invalid subscription token"}""")])]
        )
    )
    fun getDeadLetters(
        @PathVariable @NotBlank id: String,
        @RequestParam(defaultValue = "0") @Min(0) page: Int,
        @RequestParam(defaultValue = "10") @Min(1) size: Int,
        @RequestHeader("X-Sub-Token") @NotBlank token: String
    ) = subscriptionService.getDeadLetters(id, page, size, token)

    @PostMapping("/subscriptions/{id}/dead-letters/{messageId}/requeue")
    @ResponseStatus(HttpStatus.ACCEPTED)
    @Operation(summary = "Reencolar desde DLQ", description = "Mueve un mensaje de DLQ a la cola principal")
    @ApiResponses(
        ApiResponse(responseCode = "202", description = "Mensaje reencolado"),
        ApiResponse(
            responseCode = "401",
            description = "Token de suscripcion invalido",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"401 UNAUTHORIZED","message":"Invalid subscription token"}""")])]
        ),
        ApiResponse(
            responseCode = "404",
            description = "Dead letter no encontrado",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"404 NOT_FOUND","message":"Dead letter 'msg_950d117f' is not pending for subscription 'topic-demo-v1-sub-procesador'"}""")])]
        )
    )
    fun requeueDeadLetter(
        @PathVariable @NotBlank id: String,
        @PathVariable @NotBlank messageId: String,
        @RequestHeader("X-Sub-Token") @NotBlank token: String
    ) = subscriptionService.requeue(id, messageId, token)

    @GetMapping("/subscriptions/{id}/conf-resilience")
    @Operation(summary = "Obtener conf-resilience", description = "Obtiene la policy de resiliencia PUSH para la suscripcion")
    fun getConfResilience(@PathVariable @NotBlank id: String): ConfResilienceResponse =
        ConfResilienceResponse.from(confResilienceService.getForSubscription(id))

    @PutMapping("/subscriptions/{id}/conf-resilience")
    @Operation(summary = "Actualizar conf-resilience", description = "Actualiza la policy de resiliencia PUSH para la suscripcion")
    @ApiResponses(
        ApiResponse(responseCode = "200", description = "Policy actualizada"),
        ApiResponse(
            responseCode = "401",
            description = "Token de suscripcion invalido",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"401 UNAUTHORIZED","message":"Invalid subscription token"}""")])]
        )
    )
    fun updateConfResilience(
        @PathVariable @NotBlank id: String,
        @RequestHeader("X-Sub-Token") @NotBlank token: String,
        @RequestBody @Valid body: ConfResilienceUpdateRequest
    ): ConfResilienceResponse =
        ConfResilienceResponse.from(
            confResilienceService.updateForSubscription(
                id,
                token,
                body.toServiceRequest()
            )
        )

    @DeleteMapping("/subscriptions/{id}/conf-resilience")
    @Operation(summary = "Reset conf-resilience", description = "Resetea la policy de resiliencia PUSH a defaults para la suscripcion")
    @ApiResponses(
        ApiResponse(responseCode = "200", description = "Policy reseteada"),
        ApiResponse(
            responseCode = "401",
            description = "Token de suscripcion invalido",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"401 UNAUTHORIZED","message":"Invalid subscription token"}""")])]
        )
    )
    fun resetConfResilience(
        @PathVariable @NotBlank id: String,
        @RequestHeader("X-Sub-Token") @NotBlank token: String
    ): ConfResilienceResponse =
        ConfResilienceResponse.from(confResilienceService.resetForSubscription(id, token))
}

data class SubscriptionRequest(
    @field:NotBlank
    @field:Pattern(
        regexp = "^[A-Za-z]+(-[A-Za-z]+)*$",
        message = "topicId must contain only letters and hyphens"
    )
    @field:Size(max = 50, message = "topicId must not exceed 50 characters")
    @field:Schema(description = "Topic de la suscripcion (kebab-case, max 50)", example = "topic-demo")
    val topicId: String,
    @field:Min(1)
    @field:Schema(description = "Version del topic", example = "1")
    val topicVersion: Int,
    @field:NotBlank
    @field:Pattern(
        regexp = "^[A-Za-z]+(-[A-Za-z]+)*$",
        message = "nameSub must contain only letters and hyphens"
    )
    @field:Size(max = 50, message = "nameSub must not exceed 50 characters")
    @field:Schema(description = "Nombre de suscripcion (kebab-case, max 50)", example = "sub-procesador")
    val nameSub: String,
    @field:NotBlank
    @field:Schema(description = "Descripcion funcional", example = "Procesador principal por PULL")
    val description: String,
    @field:Pattern(regexp = "PULL|PUSH", flags = [Pattern.Flag.CASE_INSENSITIVE])
    @field:Schema(description = "Tipo de suscripcion", allowableValues = ["PULL", "PUSH"], example = "PULL")
    val type: String, // PUSH o PULL [cite: 54]
    @field:Schema(description = "Webhook destino para PUSH; null/blank para PULL", example = "http://example.com/webhooks/events")
    val urlRest: String?,
    @field:Min(1)
    @field:Max(100)
    @field:Schema(description = "Maximo de reintentos PUSH", example = "10")
    val maxRetries: Int = 10,
    @field:Min(1)
    @field:Max(1000)
    @field:Schema(description = "Maxima cantidad de entregas PULL antes de mover automatico a DLQ. Si no se informa, usa default global", example = "10")
    val maxDeliveryCountPull: Int? = null
) {
    @AssertTrue(message = "urlRest is required for PUSH and must be null/blank for PULL")
    fun isUrlRestConsistentWithType(): Boolean {
        val normalized = type.trim().uppercase(Locale.ROOT)
        return if (normalized == "PUSH") !urlRest.isNullOrBlank() else urlRest.isNullOrBlank()
    }
}

data class AckRequest(
    @field:Pattern(regexp = "PROCESSED|REJECT", flags = [Pattern.Flag.CASE_INSENSITIVE])
    @field:Schema(description = "Accion de ACK", allowableValues = ["PROCESSED", "REJECT"], example = "PROCESSED")
    val action: String
) // PROCESSED o REJECT [cite: 89, 94]

data class UpdateSubscriptionRequest(
    @field:NotBlank
    @field:Schema(description = "Nueva descripcion", example = "Procesador principal actualizado")
    val description: String,
    @field:Pattern(regexp = "ACTIVE|INACTIVE", flags = [Pattern.Flag.CASE_INSENSITIVE])
    @field:Schema(description = "Estado administrable por API de update. Valores permitidos aqui: ACTIVE/INACTIVE. El estado PAUSED_BY_SYSTEM es tecnico y lo gestiona solo el sistema de resiliencia PUSH.", allowableValues = ["ACTIVE", "INACTIVE", "PAUSED_BY_SYSTEM"], example = "ACTIVE")
    val status: String,
    @field:Schema(description = "Nuevo webhook para PUSH; null/blank para PULL", example = "http://example.com/webhooks/events")
    val urlRest: String? = null,
    @field:Min(1)
    @field:Max(1000)
    @field:Schema(description = "Nuevo maximo de entregas PULL antes de DLQ automatico. Si no se informa, conserva el valor actual", example = "10")
    val maxDeliveryCountPull: Int? = null
)

data class SubscriptionResponse(
    @field:Schema(example = "topic-demo-v1-sub-procesador")
    val id: String,
    @field:Schema(example = "topic-demo")
    val topicId: String,
    @field:Schema(example = "1")
    val topicVersion: Int,
    @field:Schema(example = "sub-procesador")
    val nameSub: String,
    @field:Schema(example = "1")
    val version: Int,
    val description: String?,
    val type: String,
    val urlRest: String?,
    @field:Schema(example = "10")
    val maxRetries: Int,
    @field:Schema(example = "10")
    val maxDeliveryCountPull: Int,
    @field:Schema(description = "Canal principal de entrega de la suscripcion", example = "q.topic-demo.v1.sub-procesador")
    val deliveryChannel: String,
    @field:Schema(description = "Canal de mensajes no procesables / dead letter", example = "q.topic-demo.v1.sub-procesador.dlq")
    val deadLetterChannel: String,
    @field:Schema(description = "Estado de la suscripcion: ACTIVE, INACTIVE o PAUSED_BY_SYSTEM", allowableValues = ["ACTIVE", "INACTIVE", "PAUSED_BY_SYSTEM"], example = "ACTIVE")
    val status: String
) {
    companion object {
        fun from(subscription: Subscription): SubscriptionResponse =
            SubscriptionResponse(
                id = subscription.id,
                topicId = subscription.topicId,
                topicVersion = subscription.topicVersion,
                nameSub = subscription.nameSub,
                version = subscription.version,
                description = subscription.description,
                type = subscription.type,
                urlRest = subscription.urlRest,
                maxRetries = subscription.maxRetries,
                maxDeliveryCountPull = subscription.maxDeliveryCountPull,
                deliveryChannel = subscription.mainQueue,
                deadLetterChannel = subscription.dlq,
                status = subscription.status
            )
    }
}

data class SubscriptionCreatedResponse(
    @field:Schema(example = "topic-demo-v1-sub-procesador")
    val id: String,
    @field:Schema(example = "topic-demo")
    val topicId: String,
    @field:Schema(example = "1")
    val topicVersion: Int,
    @field:Schema(example = "sub-procesador")
    val nameSub: String,
    @field:Schema(example = "1")
    val version: Int,
    val description: String?,
    val type: String,
    val urlRest: String?,
    @field:Schema(example = "10")
    val maxRetries: Int,
    @field:Schema(example = "10")
    val maxDeliveryCountPull: Int,
    @field:Schema(description = "Canal principal de entrega", example = "q.topic-demo.v1.sub-procesador")
    val deliveryChannel: String,
    @field:Schema(description = "Canal dead letter", example = "q.topic-demo.v1.sub-procesador.dlq")
    val deadLetterChannel: String,
    @field:Schema(description = "Token de la suscripcion; solo se devuelve al crear", example = "6f838a72-97eb-45f4-9335-d931ea425e88")
    val token: String,
    @field:Schema(example = "ACTIVE")
    val status: String
) {
    companion object {
        fun from(subscription: Subscription): SubscriptionCreatedResponse =
            SubscriptionCreatedResponse(
                id = subscription.id,
                topicId = subscription.topicId,
                topicVersion = subscription.topicVersion,
                nameSub = subscription.nameSub,
                version = subscription.version,
                description = subscription.description,
                type = subscription.type,
                urlRest = subscription.urlRest,
                maxRetries = subscription.maxRetries,
                maxDeliveryCountPull = subscription.maxDeliveryCountPull,
                deliveryChannel = subscription.mainQueue,
                deadLetterChannel = subscription.dlq,
                token = subscription.token,
                status = subscription.status
            )
    }
}

data class ConfResilienceUpdateRequest(
    val enabled: Boolean,
    @field:Min(1)
    val timeoutMs: Long,
    val retry: RetryConfigRequest,
    val circuitBreaker: CircuitBreakerConfigRequest,
    val bulkhead: BulkheadConfigRequest,
    val updatedBy: String? = null
) {
    fun toServiceRequest(): ConfResilienceRequest = ConfResilienceRequest(
        enabled = enabled,
        timeoutMs = timeoutMs,
        retry = com.smg.meg.service.RetryRequest(
            maxAttempts = retry.maxAttempts,
            initialBackoffMs = retry.initialBackoffMs,
            multiplier = retry.multiplier,
            maxBackoffMs = retry.maxBackoffMs,
            jitterFactor = retry.jitterFactor
        ),
        circuitBreaker = com.smg.meg.service.CircuitBreakerRequest(
            failureRateThreshold = circuitBreaker.failureRateThreshold,
            slowCallRateThreshold = circuitBreaker.slowCallRateThreshold,
            slowCallDurationMs = circuitBreaker.slowCallDurationMs,
            minimumNumberOfCalls = circuitBreaker.minimumNumberOfCalls,
            slidingWindowSize = circuitBreaker.slidingWindowSize,
            waitDurationInOpenStateMs = circuitBreaker.waitDurationInOpenStateMs,
            permittedCallsInHalfOpenState = circuitBreaker.permittedCallsInHalfOpenState
        ),
        bulkhead = com.smg.meg.service.BulkheadRequest(
            maxConcurrentCalls = bulkhead.maxConcurrentCalls,
            maxWaitDurationMs = bulkhead.maxWaitDurationMs
        ),
        updatedBy = updatedBy
    )
}

data class RetryConfigRequest(
    @field:Min(1)
    val maxAttempts: Int,
    @field:Min(0)
    val initialBackoffMs: Long,
    val multiplier: Double,
    @field:Min(0)
    val maxBackoffMs: Long,
    val jitterFactor: Double
)

data class CircuitBreakerConfigRequest(
    val failureRateThreshold: Float,
    val slowCallRateThreshold: Float,
    @field:Min(1)
    val slowCallDurationMs: Long,
    @field:Min(1)
    val minimumNumberOfCalls: Int,
    @field:Min(1)
    val slidingWindowSize: Int,
    @field:Min(1)
    val waitDurationInOpenStateMs: Long,
    @field:Min(1)
    val permittedCallsInHalfOpenState: Int
)

data class BulkheadConfigRequest(
    @field:Min(1)
    val maxConcurrentCalls: Int,
    @field:Min(0)
    val maxWaitDurationMs: Long
)

data class ConfResilienceResponse(
    val id: String,
    val subscriptionId: String,
    val enabled: Boolean,
    val timeoutMs: Long,
    val retry: com.smg.meg.model.document.RetryConf,
    val circuitBreaker: com.smg.meg.model.document.CircuitBreakerConf,
    val bulkhead: com.smg.meg.model.document.BulkheadConf,
    val updatedAt: String,
    val updatedBy: String
) {
    companion object {
        fun from(document: ConfResilience): ConfResilienceResponse = ConfResilienceResponse(
            id = document.id,
            subscriptionId = document.subscriptionId,
            enabled = document.enabled,
            timeoutMs = document.timeoutMs,
            retry = document.retry,
            circuitBreaker = document.circuitBreaker,
            bulkhead = document.bulkhead,
            updatedAt = document.updatedAt.toString(),
            updatedBy = document.updatedBy
        )
    }
}