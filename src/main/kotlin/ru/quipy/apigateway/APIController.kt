package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.logic.PaymentMetrics
import java.time.Duration
import java.util.*

@RestController
class APIController {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @Autowired
    private lateinit var paymentMetrics: PaymentMetrics

    private val userRateLimiter = SlidingWindowRateLimiter(
        1000,
        Duration.ofSeconds(1),
    )

    private val orderRateLimiter = SlidingWindowRateLimiter(
        1000,
        Duration.ofSeconds(1),
    )

    private val processRateLimiter = SlidingWindowRateLimiter(
        1000,
        Duration.ofSeconds(1),
    )

    private val avgProcessingTime = 10000 // ms

    @PostMapping("/users")
    suspend fun createUser(@RequestBody req: CreateUserRequest): ResponseEntity<User> {
        userRateLimiter.tickSuspend()

        return ResponseEntity.ok(User(UUID.randomUUID(), req.name))
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    suspend fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): ResponseEntity<Order> {
        orderRateLimiter.tickSuspend()

        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )

        return ResponseEntity.ok(orderRepository.save(order))
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    suspend fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): ResponseEntity<PaymentSubmissionDto> {
        orderRateLimiter.tickSuspend()

        val paymentId = UUID.randomUUID()

        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")

        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)

        return ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}