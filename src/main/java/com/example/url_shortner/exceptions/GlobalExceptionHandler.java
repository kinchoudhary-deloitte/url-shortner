package com.example.url_shortner.exceptions;

import com.example.demo.model.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.OffsetDateTime;
import java.util.List;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(LinkNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(LinkNotFoundException ex) {
        String requestId = MDC.get("requestId");

        var error = new ErrorResponse();
        error.setCode(LogCodes.WARN_LINK_NOT_FOUND);
        error.setMessage(ex.getMessage());
        error.setRequestId(requestId);
        error.setTimestamp(OffsetDateTime.now());

        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
    }

    @ExceptionHandler(DuplicateCodeException.class)
    public ResponseEntity<ErrorResponse> handleDuplicateCode(DuplicateCodeException ex) {
        String requestId = MDC.get("requestId");

        var error = new ErrorResponse();
        error.setCode(LogCodes.WARN_DUPLICATE_CODE);
        error.setMessage(ex.getMessage());
        error.setRequestId(requestId);
        error.setTimestamp(OffsetDateTime.now());

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }

    @ExceptionHandler(InvalidUrlException.class)
    public ResponseEntity<ErrorResponse> handleInvalidUrl(InvalidUrlException ex) {
        String requestId = MDC.get("requestId");

        var error = new ErrorResponse();
        error.setCode(LogCodes.WARN_INVALID_URL);
        error.setMessage(ex.getMessage());
        error.setRequestId(requestId);
        error.setTimestamp(OffsetDateTime.now());

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationErrors(MethodArgumentNotValidException ex) {
        String requestId = MDC.get("requestId");
        List<String> details = ex.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + ": " + f.getDefaultMessage())
                .toList();

        log.warn("level=WARN code={} event=validation_failed requestId={} errors={}",
                LogCodes.WARN_INVALID_URL, requestId, details);

        var error = new ErrorResponse();
        error.setCode(LogCodes.WARN_INVALID_URL);
        error.setMessage("Request validation failed");
        error.setRequestId(requestId);
        error.setTimestamp(OffsetDateTime.now());
        error.setDetails(details);

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception ex) {
        String requestId = MDC.get("requestId");

        log.error("level=ERROR code={} event=internal_server_error requestId={} exception={} message=\"{}\"",
                LogCodes.ERR_INTERNAL_SERVER, requestId, ex.getClass().getSimpleName(), ex.getMessage(), ex);

        var error = new ErrorResponse();
        error.setCode(LogCodes.ERR_INTERNAL_SERVER);
        error.setMessage("An unexpected error occurred processing your request.");
        error.setRequestId(requestId);
        error.setTimestamp(OffsetDateTime.now());

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
    }
}