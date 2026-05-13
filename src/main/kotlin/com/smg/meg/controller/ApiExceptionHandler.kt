package com.smg.meg.controller

import jakarta.validation.ConstraintViolationException
import io.swagger.v3.oas.annotations.media.Schema
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.MissingRequestHeaderException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException
import org.springframework.web.server.ResponseStatusException

@RestControllerAdvice
class ApiExceptionHandler {

    @ExceptionHandler(MissingRequestHeaderException::class)
    fun handleMissingHeader(ex: MissingRequestHeaderException): ResponseEntity<ErrorBody> =
        ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
            ErrorBody(
                error = "VALIDATION_ERROR",
                message = "Missing required header: ${ex.headerName}"
            )
        )

    @ExceptionHandler(ConstraintViolationException::class)
    fun handleConstraintViolation(ex: ConstraintViolationException): ResponseEntity<ErrorBody> {
        val detail = ex.constraintViolations.firstOrNull()?.message ?: "Invalid request parameter"
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
            ErrorBody(
                error = "VALIDATION_ERROR",
                message = detail
            )
        )
    }

    @ExceptionHandler(MethodArgumentNotValidException::class)
    fun handleMethodArgumentNotValid(ex: MethodArgumentNotValidException): ResponseEntity<ErrorBody> {
        val detail = ex.bindingResult.fieldErrors.firstOrNull()?.let { "${it.field}: ${it.defaultMessage}" }
            ?: "Invalid request body"
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
            ErrorBody(
                error = "VALIDATION_ERROR",
                message = detail
            )
        )
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException::class)
    fun handleTypeMismatch(ex: MethodArgumentTypeMismatchException): ResponseEntity<ErrorBody> =
        ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
            ErrorBody(
                error = "VALIDATION_ERROR",
                message = "Invalid value for '${ex.name}'"
            )
        )

    @ExceptionHandler(ResponseStatusException::class)
    fun handleResponseStatus(ex: ResponseStatusException): ResponseEntity<ErrorBody> =
        ResponseEntity.status(ex.statusCode).body(
            ErrorBody(
                error = ex.statusCode.toString(),
                message = ex.reason ?: "Request failed"
            )
        )

    @ExceptionHandler(Exception::class)
    fun handleGeneric(ex: Exception): ResponseEntity<ErrorBody> =
        ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
            ErrorBody(
                error = "INTERNAL_ERROR",
                message = "Unexpected server error"
            )
        )
}

data class ErrorBody(
    @field:Schema(description = "Codigo de error", example = "VALIDATION_ERROR")
    val error: String,
    @field:Schema(description = "Detalle del error", example = "Missing required header: Idempotency-Key")
    val message: String
)

