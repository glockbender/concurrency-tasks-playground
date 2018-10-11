package com.example.springrabbittest

import com.example.springrabbittest.config.RabbitConfig
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service

@Service
@Qualifier("sender")
class TaskSender(
        private val rabbitTemplate: RabbitTemplate
) {

    fun send(data: SuperData) {
        rabbitTemplate.convertAndSend(RabbitConfig.REPORTS_EXCHANGE, RabbitConfig.CC_ROUTING_KEY, data)

    }

}