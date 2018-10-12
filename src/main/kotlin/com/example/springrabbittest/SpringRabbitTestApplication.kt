package com.example.springrabbittest

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

@SpringBootApplication
class SpringRabbitTestApplication

fun main(args: Array<String>) {
    val ctx = runApplication<SpringRabbitTestApplication>(*args)
    val service = ctx.getBean(SuperTaskService::class.java)
    val executorService = ctx.getBean(ExecutorService::class.java)
    val sender = ctx.getBean(TaskSender::class.java)

    val cnt = AtomicInteger(1)

    fun generateData(rnd: ThreadLocalRandom, isIncrement: Boolean = true): SuperData {
        val originId = (if (isIncrement) cnt.getAndIncrement() else cnt.get()) - (if (rnd.nextBoolean()) 1 else 0)
        val id = rnd.nextInt(originId, originId + 2)
        val strValue = if (rnd.nextBoolean()) "a" else "b" + if (rnd.nextBoolean()) "id" else ""
        return SuperData(id, strValue)
    }

    //5 потоков с постоянно летящими рандомными данными
    for (i in 1..5) {
        executorService.submit {
            val rnd = ThreadLocalRandom.current()

            while (true) {
                Thread.sleep(500 * i + rnd.nextLong(0L, 1000L))
                val data = generateData(rnd)
                if (rnd.nextBoolean()) {
                    service.processHighPriority(data)
                } else {
                    service.processLowPriority(data)
                }
            }
        }
    }

    //90 быстро летящих уникальных данных. Подразумевают нагрузку на старте
    executorService.submit {
        var i = 10
        while (i < 100) {
            Thread.sleep(100)
            service.processAsync(SuperData(i++, UUID.randomUUID().toString()), {}, DataPriority.LOW)
        }
    }

    //20 тасок из рэббита
    executorService.submit {
        val rnd = ThreadLocalRandom.current()
        for (i in 1..20) {
            Thread.sleep(2000 + rnd.nextLong(0L, 1000L))
            sender.send(generateData(rnd, rnd.nextBoolean()))
        }
    }

    //Поток из 5 запросов для одной и той же таски
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
