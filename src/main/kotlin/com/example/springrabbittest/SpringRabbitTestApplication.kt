package com.example.springrabbittest

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

@SpringBootApplication
class SpringRabbitTestApplication

fun booleanRandom(): Boolean = true

fun main(args: Array<String>) {
    val ctx = runApplication<SpringRabbitTestApplication>(*args)
    val service = ctx.getBean(SuperTaskService::class.java)
    val executorService = ctx.getBean(ExecutorService::class.java)

    val cnt = AtomicInteger(1)

    fun makeRandom() {
        executorService.submit {
            val rnd = ThreadLocalRandom.current()

            fun rndValue(): String = if (rnd.nextBoolean()) "a" else "b"

            fun rndId(): Int {
                val origin = cnt.getAndIncrement() - if (rnd.nextBoolean()) 1 else 0
                return rnd.nextInt(origin, origin + 2)
            }

            while (true) {
                val data = SuperData(rndId(), rndValue())
                if (rnd.nextBoolean()) {
                    service.processHighPriority(data)
                } else {
                    service.processLowPriority(data)
                }
                Thread.sleep(500 + rnd.nextLong(0L, 1000L))
            }
        }
    }

    for (i in 1..5) {
        makeRandom()
    }

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
