package com.example.springrabbittest

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.util.concurrent.ExecutorService

@SpringBootApplication
class SpringRabbitTestApplication


fun main(args: Array<String>) {
    val ctx = runApplication<SpringRabbitTestApplication>(*args)
    val service = ctx.getBean(SuperTaskService::class.java)
    val executorService = ctx.getBean(ExecutorService::class.java)

    executorService.submit {
        val data = SuperData(2, "a")
        service.processHighPriority(data)
    }

    executorService.submit {
        val data = SuperData(3, "a")
        service.processHighPriority(data)
    }

    executorService.submit {
        val data = SuperData(1, "a")
        service.processLowPriority(data)
    }

    executorService.submit {
        val data = SuperData(1, "a")
        service.processHighPriority(data)
    }

    Thread.sleep(100)

    executorService.submit {
        val data = SuperData(1, "a")
        service.processHighPriority(data)
    }

    Thread.sleep(200)

    executorService.submit {
        val data = SuperData(1, "a")
        service.processLowPriority(data)
    }

    Thread.sleep(300)

    executorService.submit {
        val data = SuperData(1, "a")
        service.processLowPriority(data)
    }

}
