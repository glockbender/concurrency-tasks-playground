package com.example.springrabbittest

import com.example.springrabbittest.config.RabbitConfig
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.stereotype.Service

@Service
open class CCListener(
        private val service: SuperTaskService
) {

    @RabbitListener(queues = [RabbitConfig.CC_QUEUE], autoStartup = "true")
    fun handle(data: SuperData) {
        println("Listen data: $data")
        val result = service.executeSuperDataFlow(data, DataPriority.LOW)
        println("___________________________FROM RABBIT LISTENER___________________________")
        println(result)
    }

}