package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.ktor.client.HttpClient
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.ConnectionPool
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random

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
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .connectionPool(ConnectionPool(150, 10000, TimeUnit.MILLISECONDS))
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .build()

    private val httpClient = java.net.http.HttpClient.newBuilder()
        .version(java.net.http.HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(1))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1),
    )

    private val semaphore: Semaphore = Semaphore(parallelRequests)

    override suspend fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
    ) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val baseDelay = 100L // ms
        val maxDelay = 16000L // ms
        val quantileProcessingTime = 1000 // ms

        repeat(8) { attempt ->
            var shouldRetry = false

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

                    val response = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()

                    try {
                        mapper.readValue(response.body(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error(
                            "[$accountName] [ERROR] txId=$transactionId payment=$paymentId code=${response.statusCode()} reason=${response.body()}"
                        )
                        ExternalSysResponse(
                            transactionId.toString(),
                            paymentId.toString(),
                            false,
                            e.message
                        )
                    }
                } catch (e: Exception) {
                    when (e) {
                        is SocketTimeoutException -> {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        }

                        is ShouldRetryException -> {
                            throw e
                        }

                        is InterruptedIOException -> {
                            shouldRetry = true
                        }

                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                        }
                    }
                }
            }

            if (!shouldRetry) {
                return
            }

            val backoff = baseDelay * (2.0.pow(attempt)).toLong()
            val cappedBackoff = min(backoff, maxDelay)
            val jittered = Random.nextLong(baseDelay, cappedBackoff + 1)

            logger.warn("transaction $transactionId, payment $paymentId, attempt $attempt, going for delay $jittered")
            delay(jittered)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
