package com.smg.meg.controller

import com.smg.meg.model.document.TopicSchemaValidation
import com.smg.meg.service.TopicSchemaValidationService
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
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotEmpty
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Size
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@Tag(name = "Topic Schema Validation", description = "Gestion de schema JSON por topic/version")
class TopicSchemaValidationController(
    private val service: TopicSchemaValidationService
) {

    @PostMapping("/topics/{id}/v{version}/schema-validation")
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Crear schema validation", description = "Crea una configuracion de validacion de schema para topic/version")
    @ApiResponses(
        ApiResponse(
            responseCode = "201",
            description = "Schema creado",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [
                        ExampleObject(
                            value = """{"id":"topic-demo:v1","topicId":"topic-demo","topicVersion":1,"enabled":true,"description":"Schema topic-demo v1","schema":{"schemaVersion":"http://json-schema.org/draft-07/schema#","type":"object","required":["amount","account","document"]}}"""
                        )
                    ]
                )
            ]
        ),
        ApiResponse(responseCode = "400", description = "Request invalido", content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"400 BAD_REQUEST","message":"schema must not be empty"}""")])]),
        ApiResponse(
            responseCode = "404",
            description = "Topic no encontrado",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"404 NOT_FOUND","message":"Topic 'topic-demo' not found"}""")])]
        ),
        ApiResponse(
            responseCode = "409",
            description = "Schema ya existente",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"409 CONFLICT","message":"Schema validation already exists for topic 'topic-demo' version 1"}""")])]
        )
    )
    fun create(
        @PathVariable
        @NotBlank
        @Pattern(
            regexp = "^[A-Za-z]+(-[A-Za-z]+)*$",
            message = "topic id must contain only letters and hyphens"
        )
        @Size(max = 50, message = "topic id must not exceed 50 characters")
        @Parameter(description = "ID de topic", example = "topic-demo")
        id: String,
        @Parameter(description = "Version del topic", example = "1")
        @PathVariable @Min(1) version: Int,
        @SwaggerRequestBody(
            required = true,
            content = [
                Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = TopicSchemaValidationRequest::class),
                    examples = [
                        ExampleObject(
                            value = """{"enabled":true,"description":"Schema topic-demo v1","schema":{"schemaVersion":"http://json-schema.org/draft-07/schema#","type":"object","required":["amount","account","document"],"properties":{"amount":{"type":"number","exclusiveMinimum":0},"account":{"type":"string","minLength":22,"maxLength":22},"document":{"type":"string","minLength":7,"maxLength":8,"pattern":"^[0-9]+$"}}}}"""
                        )
                    ]
                )
            ]
        )
        @RequestBody @Valid request: TopicSchemaValidationRequest
    ): TopicSchemaValidation = service.create(id, version, request.enabled, request.description, request.schema)

    @PutMapping("/topics/{id}/v{version}/schema-validation")
    @Operation(summary = "Actualizar schema validation", description = "Actualiza el schema y metadata de validacion para topic/version")
    @ApiResponses(
        ApiResponse(responseCode = "200", description = "Schema actualizado"),
        ApiResponse(
            responseCode = "400",
            description = "Request invalido",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"400 BAD_REQUEST","message":"schema must not be empty"}""")])]
        ),
        ApiResponse(
            responseCode = "404",
            description = "Schema o topic no encontrado",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"404 NOT_FOUND","message":"Schema validation not found for topic 'topic-demo' version 1"}""")])]
        )
    )
    fun update(
        @PathVariable
        @NotBlank
        @Pattern(
            regexp = "^[A-Za-z]+(-[A-Za-z]+)*$",
            message = "topic id must contain only letters and hyphens"
        )
        @Size(max = 50, message = "topic id must not exceed 50 characters")
        id: String,
        @PathVariable @Min(1) version: Int,
        @RequestBody @Valid request: TopicSchemaValidationRequest
    ): TopicSchemaValidation = service.update(id, version, request.enabled, request.description, request.schema)

    @GetMapping("/topics/{id}/v{version}/schema-validation")
    @Operation(summary = "Obtener schema validation", description = "Obtiene la configuracion de schema validation de un topic/version")
    @ApiResponses(
        ApiResponse(responseCode = "200", description = "Schema encontrado"),
        ApiResponse(
            responseCode = "404",
            description = "Schema no encontrado",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"404 NOT_FOUND","message":"Schema validation not found for topic 'topic-demo' version 1"}""")])]
        )
    )
    fun get(
        @PathVariable
        @NotBlank
        @Pattern(
            regexp = "^[A-Za-z]+(-[A-Za-z]+)*$",
            message = "topic id must contain only letters and hyphens"
        )
        @Size(max = 50, message = "topic id must not exceed 50 characters")
        id: String,
        @PathVariable @Min(1) version: Int
    ): TopicSchemaValidation = service.get(id, version)
}

data class TopicSchemaValidationRequest(
    @field:Schema(description = "Flag para activar validacion", example = "true", defaultValue = "true")
    val enabled: Boolean = true,
    @field:Schema(description = "Descripcion del schema", example = "Schema topic-demo v1")
    val description: String? = null,
    @field:NotEmpty(message = "schema must not be empty")
    @field:Schema(
        description = "JSON Schema draft-07",
        example = """{"schemaVersion":"http://json-schema.org/draft-07/schema#","type":"object","required":["monto","cbu","du"]}"""
    )
    val schema: Map<String, Any>
)
