package ru.quipy.payments.logic

import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component


@Component
class PaymentMetrics(
    private val meterRegistry: MeterRegistry,
) {
    private val metricRequestDuration = DistributionSummary
        .builder("race_m3400_01_avg_payment_processing_time")
        .description("Average payment processing time")
        .publishPercentiles(0.5, 0.75, 0.95, 0.99)
        .publishPercentileHistogram()
        .register(meterRegistry)

    fun observeRequestDuration(milliseconds: Long) =
        metricRequestDuration.record(milliseconds.toDouble() / 1000.0)
}