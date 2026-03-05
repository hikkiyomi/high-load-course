package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.time.withTimeoutOrNull
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.*

class ShouldRetryException(val retryAfter: Long)
    : Exception("Retry after $retryAfter timestamp.")

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = HttpRequest.BodyPublishers.noBody()
        val mapper = ObjectMapper().registerKotlinModule()

        init {
            System.setProperty("jdk.httpclient.connectionPoolSize", "1000")
            System.setProperty("jdk.httpclient.keepalive.timeout", "300")
        }
    }


    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val httpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(1))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1),
    )

    private val semaphore: Semaphore = Semaphore(parallelRequests)

    private val quantileProcessingTime = 200 // ms

    private val hedgeRequests = 2
    private val hedgeTimeout = Duration.ofMillis(120)

    private val warmupScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    init {
        warmupConnections()
    }

    override suspend fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
    ) {
        coroutineScope {
            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            val transactionId = UUID.randomUUID()
            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val activeRequests = mutableListOf<Deferred<ExternalSysResponse>>()

            try {
                val primary = async { doRequest(transactionId, paymentId, amount, deadline) }
                activeRequests.add(primary)

                repeat(hedgeRequests) {
                    val remainingTime = deadline - now()

                    if (quantileProcessingTime >= remainingTime) {
                        return@repeat
                    }

                    val completed = withTimeoutOrNull(hedgeTimeout) {
                        primary.await()
                    }

                    if (completed != null) {
                        return@repeat
                    }

                    val hedge = async { doRequest(transactionId, paymentId, amount, deadline) }
                    activeRequests.add(hedge)
                }

                select {
                    activeRequests.forEach { deferred ->
                        deferred.onAwait { it }
                    }
                }
            } finally {
                activeRequests.forEach {
                    it.cancel()
                }
            }
        }
    }

    private suspend fun doRequest(
        transactionId: UUID,
        paymentId: UUID,
        amount: Int,
        deadline: Long,
    ): ExternalSysResponse {
        semaphore.withPermit {
            try {
                rateLimiter.tickSuspend()

                if (deadline < now() + quantileProcessingTime) {
                    throw ShouldRetryException(now() + quantileProcessingTime)
                }

                val request =
                    HttpRequest.newBuilder()
                        .uri(
                            URI.create(
                                "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
                            )
                        )
                        .POST(emptyBody)
                        .timeout(Duration.ofSeconds(deadline - now()))
                        .build()

                val response = httpClient
                    .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .await()

                try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error(
                        "[$accountName] [ERROR] txId=$transactionId payment=$paymentId code=${response.statusCode()} reason=${response.body()}"
                    )

                    return ExternalSysResponse(
                        transactionId.toString(),
                        paymentId.toString(),
                        false,
                        e.message,
                    )
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException, is HttpTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    }

                    is ShouldRetryException -> {
                        throw e
                    }

                    is CancellationException -> {
                        logger.error("[$accountName] Hedged requests cancelled.")
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    }
                }
            }
        }

        return ExternalSysResponse(
            transactionId.toString(),
            paymentId.toString(),
            true,
            "success",
        )
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun warmupConnections() {
        repeat(1000) {
            warmupScope.launch {
                try {
                    val request = HttpRequest.newBuilder()
                        .uri(URI.create("http://$paymentProviderHostPort"))
                        .GET()
                        .timeout(Duration.ofSeconds(1))
                        .build()

                    httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding()).thenRun {
                        logger.info("[$accountName] Warmed up")
                    }
                } catch (e: Exception) {
                    logger.info("[$accountName] Warmup exception", e)
                }
            }
        }
    }
}

public fun now() = System.currentTimeMillis()
