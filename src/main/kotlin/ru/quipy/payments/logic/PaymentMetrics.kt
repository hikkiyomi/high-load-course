package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component


@Component
class PaymentMetrics(
    private val meterRegistry: MeterRegistry,
) {
    private val requestDuration = DistributionSummary
        .builder("race_m3400_01_avg_payment_processing_time")
        .description("Average payment processing time")
        .publishPercentiles(0.5, 0.75, 0.95, 0.99)
        .publishPercentileHistogram()
        .register(meterRegistry)

    private val sentWithRetryAfter = Counter
        .builder("race_m3400_01_sent_with_retry_after_total")
        .description("Retry after responses")
        .register(meterRegistry)

    private val retriesCounter = Counter
        .builder("race_m3400_01_request_retries_total")
        .description("Retries on external client")
        .register(meterRegistry)

    fun observeRequestDuration(milliseconds: Long) =
        requestDuration.record(milliseconds.toDouble() / 1000.0)

    fun metricSentWithRetryAfterInc() =
        sentWithRetryAfter.increment()

    fun metricRetriesCounterInc() =
        retriesCounter.increment()
}