package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CompositeRateLimiter
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.Int


data class PaymentRequest(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long,
    val callback: (Long) -> Unit,
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

    private val client = OkHttpClient.Builder().build()

    private var rateLimiter: RateLimiter
    private var ongoingWindow: OngoingWindow

    private val requestQueue = Channel<PaymentRequest>(Channel.UNLIMITED)

    init {
        rateLimiter = TokenBucketRateLimiter(
            parallelRequests,
            parallelRequests,
            requestAverageProcessingTime.toMillis(),
            TimeUnit.MILLISECONDS,
        )

        ongoingWindow = OngoingWindow(parallelRequests)

        val scope = CoroutineScope(Dispatchers.IO)

        repeat(parallelRequests) {
            scope.launch {
                processPaymentAsync()
            }
        }
    }

    private suspend fun processPaymentAsync() {
        for (request in requestQueue) {
            val paymentId = request.paymentId
            val amount = request.amount
            val paymentStartedAt = request.paymentStartedAt
            val deadline = request.deadline
            val transactionId = UUID.randomUUID()

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            try {
                val retries = 3
                var got = false
                var delay = 100

                for (i in 1..retries) {
                    if (rateLimiter.tick()) {
                        got = true
                        break
                    } else {
                        delay(delay + (Math.random() * delay).toLong())
                        delay *= 2
                    }
                }

                // do load shedding
                if (!got) {
                    throw Exception("no available tokens for this request")
                }

                ongoingWindow.acquire()

                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                val clientWithTimeout = client
                    .newBuilder()
                    .callTimeout(deadline - now(), TimeUnit.MILLISECONDS)
                    .build()

                clientWithTimeout
                    .newCall(request)
                    .execute()
                    .use { response ->
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                    }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            } finally {
                ongoingWindow.release()
                request.callback(now() - paymentStartedAt)
            }
        }
    }

    override suspend fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        callback: (Long) -> Unit,
    ) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        requestQueue.send(PaymentRequest(
            paymentId = paymentId,
            amount = amount,
            paymentStartedAt = paymentStartedAt,
            deadline = deadline,
            callback = callback,
        ))
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()