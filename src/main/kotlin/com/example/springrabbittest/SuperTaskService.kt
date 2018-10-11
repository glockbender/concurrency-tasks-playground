package com.example.springrabbittest

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


//TODO: Попробовать убрать лок, т.к. все синхронизировано. Разобраться, где не нужны Concurrent Maps
@Service
final class SuperTaskService {

    private val log = LoggerFactory.getLogger(this::class.simpleName)

    private val waits: MutableMap<SuperData, SuperDataTask> = HashMap()

    private val executing: MutableMap<SuperData, SuperDataTask> = HashMap()

    private val doneTasks: MutableMap<SuperData, SuperData> = ConcurrentHashMap()

    private val executingLatches: MutableMap<SuperDataTask, CountDownLatch> = ConcurrentHashMap()

    private val latchSubscribers: MutableMap<SuperDataTask, Int> = ConcurrentHashMap()

    private val flowExecutor = SuperDataExecutor(
            //onSubmit = { lock.withLock { waits[it.data] = it }  },
            beforeExecute = { task -> lock.withLock { waits.remove(task.data)?.let { executing[it.data] = it } } },
            afterExecute = { task -> lock.withLock { executing.remove(task.data) } }
    )

    private val lock = ReentrantLock()

    private val mainFun: (SuperData) -> SuperData = {
        log.debug("Main function start")
        val result =
                if (it.value == "a") {
                    log.debug("THIS IS FAST TASK!")
                    Thread.sleep(2000)
                    it.copy(value = "A")
                } else {
                    log.debug("THIS IS SLOW TASK!")
                    Thread.sleep(3000)
                    it.copy(value = it.value.capitalize())
                }
        log.debug("TASK COMPLETED! RESULT: {}", result)
        result
    }

    fun processLowPriority(data: SuperData): SuperData {
        log.info("START PROCESSING LOW PRIORITY DATA: {}", data)
        return executeSuperDataFlow(data, DataPriority.LOW)
    }

    fun processHighPriority(data: SuperData): SuperData {
        log.info("START PROCESSING HIGH PRIORITY DATA: {}", data)
        return executeSuperDataFlow(data, DataPriority.HIGH)
    }

    fun executeSuperDataFlow(data: SuperData, priority: DataPriority): SuperData {

        log.debug("STARTING SUPER DATA FLOW EXECUTION: {}, PRIORITY: {}", data, priority)

        lock.lock()
        printStat("START")
        try {
            //Для начала пробуем получить таску из ожидающих
            var task: SuperDataTask? = waits[data]
            var latch: CountDownLatch?
            //Если таски нет в ожидающих выполнения
            if (task == null) {
                task = executing[data]
                //Если таски нет в выполняющихся - таска новая
                if (task == null) {
                    latch = null
                    //Пробуем получить latch (можем и не успеть)
                } else {
                    latch = executingLatches[task]
                }
            } else {
                //Если текущий приоритет выше, чем в ожидающей таске - повышаем пока не поздно
                if (priority.value > task.priority.value) {
                    flowExecutor.toHighPriority(task)
                }
                //Тут !!, чтобы могло упасть в случае реальной ошибки
                latch = executingLatches[task]!!
            }

            //Если так и не получили latch - Задача новая.
            //Создаем новую таску и все остальное и отправляем поток ждать резульат
            if (latch == null) {
                latch = CountDownLatch(1)
                task = SuperDataTask(data, priority,
                        Runnable {
                            val result = mainFun.invoke(data)
                            doneTasks[data] = result
                            latch.countDown()
                        })
                log.info("SUBMITTING NEW TASK: {}", task)
                executingLatches[task] = latch
                latchSubscribers[task] = 1
                waits[data] = task
                flowExecutor.submit(task)
            } else {
                log.info("EXISTING TASK FOUND!!!")
                latchSubscribers.computeIfPresent(task!!) { _, count -> count + 1 }
            }

            lock.unlock()

            log.debug("AWAITING RESULT...")
            latch.await()
            log.debug("GETTING DATA...")
            printStat("AFTER AWAIT")
            val result = doneTasks[data]
            //После получения данных уменьшаем счетчик подписок на таску
            val subscribersCount = latchSubscribers.computeIfPresent(task) { _, count ->
                if (count == 1) null
                else count - 1
            }
            log.debug("CURRENT TASK SUBSCRIBERS: {}", subscribersCount)
            if (subscribersCount == null) {
                doneTasks.remove(data)
                executingLatches.remove(task)
            }
            log.debug("DONE!!! RESULT: {}", result)
            return result!!
        } finally {
            if (lock.isLocked)
                lock.unlock()
            printStat("FINALLY")
        }


    }

    private fun printStat(prefix: String) {
        log.debug("\n $prefix STATS: " +
                "\n waits: ${waits.size} " +
                "\n executing: ${executing.size} " +
                "\n latches: ${executingLatches.size}" +
                "\n done: ${doneTasks.size}")
    }
}