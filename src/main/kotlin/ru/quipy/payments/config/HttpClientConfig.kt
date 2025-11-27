package ru.quipy.payments.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.HttpProtocol
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider

@Configuration
class HttpClientConfig {
    @Bean
    fun getClient(): WebClient {
        val connProvider = ConnectionProvider
            .builder("payment-external")
            .maxConnections(10000)
            .build()

        val client = HttpClient
            .create(connProvider)
            .protocol(HttpProtocol.H2C)

        return WebClient
            .builder()
            .clientConnector(ReactorClientHttpConnector(client))
            .build()
    }
}