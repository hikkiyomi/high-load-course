package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CoroutineOngoingWindow
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random

class ShouldRetryException(val retryAfter: Long)
    : Exception("Retry after $retryAfter timestamp.")

data class PaymentRequest(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long,
    val callback: (Long) -> Unit,
    val onRetry: () -> Unit,
)

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val ktorClient = HttpClient(OkHttp) {
        engine {
            config {
                protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
                dispatcher = Dispatchers.IO
            }
        }

        install(HttpTimeout) {
            socketTimeoutMillis = 5 * requestAverageProcessingTime.toMillis()
            requestTimeoutMillis = 15000
        }

        install(ContentNegotiation) {
            json()
        }
    }

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1),
    )

    private val ongoingWindow = CoroutineOngoingWindow(parallelRequests)

    private val requests = Channel<PaymentRequest>(rateLimitPerSec)

    init {
        repeat(10000) {
            CoroutineScope(Dispatchers.IO).launch {
                processPaymentAsync()
            }
        }
    }

    suspend fun processPaymentAsync() {
        for (request in requests) {
            val paymentId = request.paymentId
            val amount = request.amount
            val paymentStartedAt = request.paymentStartedAt
            val deadline = request.deadline
            val callback = request.callback
            val onRetry = request.onRetry

            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            val transactionId = UUID.randomUUID()

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val baseDelay = 100L // ms
            val maxDelay = 16000L // ms
            val quantileProcessingTime = 15000 // ms

            repeat(8) { attempt ->
                var shouldRetry = false

                try {
                    ongoingWindow.acquire()
                    rateLimiter.tickSuspend()

                    if (deadline < now() + quantileProcessingTime) {
                        throw ShouldRetryException(now() + quantileProcessingTime)
                    }

                    val response: HttpResponse = ktorClient.post(
                        "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
                    ) {
                        contentType(ContentType.Application.Json)
                    }

                    val body = try {
                        mapper.readValue(response.bodyAsText(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.status.value}, reason: ${response.bodyAsText()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}, code: ${response.status.value}")

                    if (
                        !body.result &&
                        body.message?.contains("Temporary error") == false &&
                        response.status.value != 429 &&
                        response.status.value !in 500..599
                    ) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request fail")
                        }

                        callback(now())
                        return
                    }

                    if (body.result) {
                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        paymentESService.update(paymentId) {
                            it.logProcessing(true, now(), transactionId, reason = body.message)
                        }

                        callback(now())
                        return
                    }

                    shouldRetry = true
                } catch (e: Exception) {
                    when (e) {
                        is SocketTimeoutException -> {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)

                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }

                        is ShouldRetryException -> {
                            throw e
                        }

                        is InterruptedIOException, is HttpRequestTimeoutException -> {
                            shouldRetry = true
                        }

                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }

                    callback(now())
                } finally {
                    ongoingWindow.release()
                }

                if (!shouldRetry) {
                    return
                }

                val backoff = baseDelay * (2.0.pow(attempt)).toLong()
                val cappedBackoff = min(backoff, maxDelay)
                val jittered = Random.nextLong(baseDelay, cappedBackoff + 1)

                logger.warn("transaction $transactionId, payment $paymentId, attempt $attempt, going for delay $jittered")
                delay(jittered)

                onRetry()
            }

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request fail")
            }

            callback(now())
        }
    }

    override suspend fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        callback: (Long) -> Unit,
        onRetry: () -> Unit,
    ) {
        requests.send(
            PaymentRequest(
                paymentId = paymentId,
                amount = amount,
                paymentStartedAt = paymentStartedAt,
                deadline = deadline,
                callback = callback,
                onRetry = onRetry,
            ),
        )
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()