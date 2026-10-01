package com.trading.events.infrastructure.web.advice;

import com.trading.events.domain.exception.GatewayUnavailableException;
import com.trading.events.domain.exception.InsufficientFundsException;
import com.trading.events.domain.exception.InvalidEventException;
import com.trading.events.domain.exception.OverloadedException;
import com.trading.events.domain.exception.ResourceNotFoundException;
import com.trading.events.domain.exception.TransientProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebInputException;

import java.util.List;

/**
 * Traduce excepciones a respuestas RFC 7807 ({@link ProblemDetail}).
 * 429 y 503 llevan {@code Retry-After} para que los clientes apliquen backoff.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private static final String RETRY_AFTER_SECONDS = "1";

    @ExceptionHandler(InvalidEventException.class)
    public ResponseEntity<ProblemDetail> invalidEvent(InvalidEventException e) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid request", e.getMessage());
    }

    /** Errores propios del framework (ruta inexistente, JSON ilegible, método no permitido...) conservan su status. */
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ProblemDetail> responseStatus(ResponseStatusException e) {
        HttpStatus status = HttpStatus.valueOf(e.getStatusCode().value());
        String title = e instanceof ServerWebInputException ? "Invalid request" : status.getReasonPhrase();
        return problem(status, title, e.getReason() != null ? e.getReason() : title);
    }

    @ExceptionHandler(WebExchangeBindException.class)
    public ResponseEntity<ProblemDetail> validation(WebExchangeBindException e) {
        List<String> errors = e.getFieldErrors().stream()
                .map(f -> f.getField() + ": " + f.getDefaultMessage())
                .toList();
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Validation failed");
        detail.setTitle("Invalid request");
        detail.setProperty("errors", errors);
        return ResponseEntity.badRequest().body(detail);
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ProblemDetail> notFound(ResourceNotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, "Not found", e.getMessage());
    }

    @ExceptionHandler(InsufficientFundsException.class)
    public ResponseEntity<ProblemDetail> insufficientFunds(InsufficientFundsException e) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, "Insufficient funds", e.getMessage());
    }

    @ExceptionHandler(OverloadedException.class)
    public ResponseEntity<ProblemDetail> overloaded(OverloadedException e) {
        return withRetryAfter(problem(HttpStatus.TOO_MANY_REQUESTS, "System overloaded", e.getMessage()));
    }

    @ExceptionHandler({GatewayUnavailableException.class, TransientProcessingException.class})
    public ResponseEntity<ProblemDetail> unavailable(RuntimeException e) {
        return withRetryAfter(problem(HttpStatus.SERVICE_UNAVAILABLE, "Market unavailable", e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ProblemDetail> unexpected(Exception e) {
        log.error("Unexpected error", e);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error", "Unexpected error");
    }

    private static ResponseEntity<ProblemDetail> problem(HttpStatus status, String title, String detail) {
        ProblemDetail body = ProblemDetail.forStatusAndDetail(status, detail);
        body.setTitle(title);
        return ResponseEntity.status(status).body(body);
    }

    private static ResponseEntity<ProblemDetail> withRetryAfter(ResponseEntity<ProblemDetail> response) {
        return ResponseEntity.status(response.getStatusCode())
                .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
                .body(response.getBody());
    }
}
