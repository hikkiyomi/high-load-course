package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*

@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
    private val paymentMetrics: PaymentMetrics,
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override suspend fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            val callback = { timestamp: Long ->
                paymentMetrics.observeRequestDuration(timestamp - paymentStartedAt)
            }

            val onRetry = {
                paymentMetrics.metricRetriesCounterInc()
            }

            account.performPaymentAsync(
                paymentId,
                amount,
                paymentStartedAt,
                deadline,
                callback,
                onRetry,
            )
        }
    }
}