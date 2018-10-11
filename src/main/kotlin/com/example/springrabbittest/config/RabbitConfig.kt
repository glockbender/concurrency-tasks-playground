package com.example.springrabbittest.config

import org.springframework.amqp.core.*
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.validation.annotation.Validated
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Validated
@Configuration
@ConfigurationProperties(prefix = "rmq")
class RabbitConfig {

    //Настройки Rabbit из конфига
    lateinit var host: String
    var port: Int = 0
    lateinit var username: String
    lateinit var password: String

    @Bean
    fun connectionFactory(): ConnectionFactory =
            CachingConnectionFactory(host)
                    .apply {
                        port = this@RabbitConfig.port!!
                        username = this@RabbitConfig.username
                        setPassword(password)
                    }

    @Bean
    @Qualifier(REPORTS_EXCHANGE)
    fun reportsExchange(): Exchange =
            ExchangeBuilder
                    .topicExchange(REPORTS_EXCHANGE)
                    .build()


    @Bean
    @Qualifier(CC_QUEUE)
    fun ccQueue(): Queue =
            QueueBuilder
                    .durable(CC_QUEUE)
                    .build()

    @Bean
    internal fun ccBinding(
            @Qualifier(CC_QUEUE) ccQueue: Queue,
            @Qualifier(REPORTS_EXCHANGE) reportsExchange: TopicExchange
    ): Binding = BindingBuilder
            .bind(ccQueue)
            .to(reportsExchange)
            .with(CC_ROUTING_KEY)

    @Bean
    fun executorService(): ExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2)

//    @Bean
//    fun messageConverter(): MessageConverter = Jackson2JsonMessageConverter()


    companion object {

        /**
         * Имя очереди SmartReserve
         */
        const val SR_QUEUE = "sr_queue"

        /**
         * Имя очереди CallCenter
         */
        const val CC_QUEUE = "my_queue"

        /**
         * Имя очереди создания excel файлов
         */
        const val EXCEL_QUEUE = "excel_queue"

        /**
         * Routing Key SmartReserve
         */
        const val SR_ROUTING_KEY = "sr_rk"

        /**
         * Routing Key CallCenter
         */
        const val CC_ROUTING_KEY = "my_rk"

        /**
         * Routing key для очереди excel файлов
         */
        const val EXCEL_ROUTING_KEY = "excel_rk"

        /**
         * Exchange для поступающих запросов на отчет
         */
        const val REPORTS_EXCHANGE = "my_ex"

        const val DEAD_LETTER_QUEUE = "dead_letter"
    }

}