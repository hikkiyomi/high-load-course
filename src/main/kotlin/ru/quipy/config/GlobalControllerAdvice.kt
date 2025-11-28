package ru.quipy.config

import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@ControllerAdvice
class GlobalControllerAdvice {
    @ExceptionHandler(WebClientResponseException.TooManyRequests::class)
    fun handle(e: WebClientResponseException.TooManyRequests): Mono<ResponseEntity<String>> {
        val headers = HttpHeaders()

        headers.add("Retry-After", "${System.currentTimeMillis() + 1000}")

        return Mono.just(
            ResponseEntity
                .status(HttpStatus.TOO_MANY_REQUESTS)
                .headers(headers)
                .body("try again later.")
        )
    }
}
