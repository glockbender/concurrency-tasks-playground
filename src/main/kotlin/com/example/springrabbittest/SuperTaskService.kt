package com.example.springrabbittest

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Supplier
import kotlin.concurrent.withLock

@Service
final class SuperTaskService(
        private val executorService: ExecutorService
) {

    class TaskWaiter {

        private val lock = ReentrantLock()
        private val condition = lock.newCondition()
        @Volatile
        private var waiter: Boolean = true

        fun done() {
            lock.withLock {
                waiter = false
                condition.signalAll()
            }
        }

        fun awaitTask() {
            lock.withLock {
                while (waiter) {
                    condition.await()
                }
                condition.signalAll()
            }
        }
    }

    private val log = LoggerFactory.getLogger(this::class.simpleName)

    private val waits: MutableMap<SuperData, SuperDataTask> = HashMap()

    private val executing: MutableMap<SuperData, SuperDataTask> = HashMap()

    private val doneTasks: MutableMap<SuperData, SuperData> = ConcurrentHashMap()

    private val taskWaiters: MutableMap<SuperDataTask, TaskWaiter> = ConcurrentHashMap()

    private val latchSubscribers: MutableMap<SuperDataTask, Int> = ConcurrentHashMap()

    private val flowExecutor = SuperDataExecutor(
            beforeExecute = { task -> lock.withLock { waits.remove(task.data)?.let { executing[it.data] = it } } },
            afterExecute = { task -> lock.withLock { executing.remove(task.data) } }
    )

    private val lock = ReentrantLock()

    private val mainFun: (SuperData) -> SuperData = {
        log.debug("Main function start")
        val result =
                if (it.value == "a") {
                    log.debug("THIS IS FAST TASK!")
                    Thread.sleep(500)
                    it.copy(value = "A")
                } else {
                    log.debug("THIS IS SLOW TASK!")
                    Thread.sleep(750)
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

    /**
     * "Долгий", последовательный вариант пакетной обработки
     */
    fun processBatchSequentially(data: List<SuperData>, priority: DataPriority): List<SuperData> {
        val list = mutableListOf<SuperData>()
        data.forEach { list.add(executeSuperDataFlow(it, priority)) }
        return list
    }

    /**
     * Более быстрый (x3-4) способ пакетной обработки на основе [CompletableFuture].
     * Таски [CompletableFuture] создаются на основе сконфигурированного [ExecutorService],
     * т.к. в большинстве случаев [java.util.concurrent.ForkJoinPool] оказывается медленнее
     */
    fun processBatchInParallel(data: List<SuperData>, priority: DataPriority): List<SuperData> {
        val futures = data.map { CompletableFuture.supplyAsync(Supplier { executeSuperDataFlow(it, priority) }, executorService) }
        return aggregateToOneListFuture(futures).get()
    }

    /**
     * Асинхронная обработка одиночной задачи
     */
    fun processAsync(data: SuperData, successCallback: (SuperData) -> Unit, priority: DataPriority = DataPriority.LOW) {
        executorService.submit {
            successCallback.invoke(executeSuperDataFlow(data, priority))
        }
    }

    /**
     * Асинхронная пакетная обработка на основе [processBatchSequentially]
     */
    fun processBatchSequentiallyAsync(data: List<SuperData>, successCallback: (List<SuperData>) -> Unit, priority: DataPriority = DataPriority.LOW) {
        executorService.submit {
            successCallback.invoke(processBatchSequentially(data, priority))
        }
    }

    /**
     * Асинхронная пакетная обработка на основе [processBatchInParallel]
     */
    fun processBatchInParallelAsync(data: List<SuperData>, successCallback: (List<SuperData>) -> Unit, priority: DataPriority = DataPriority.LOW) {
        executorService.submit {
            successCallback.invoke(processBatchInParallel(data, priority))
        }
    }

    fun executeSuperDataFlow(data: SuperData, priority: DataPriority): SuperData {

        log.debug("STARTING SUPER DATA FLOW EXECUTION: {}, PRIORITY: {}", data, priority)

        lock.lock()
        printStat("START")
        try {
            //Для начала пробуем получить таску из ожидающих
            var task: SuperDataTask? = waits[data]
            var latch: TaskWaiter?
            //Если таски нет в ожидающих выполнения
            if (task == null) {
                //Пробуем получить из выполняющихся
                task = executing[data]
                //Если таски нет в выполняющихся - таска новая
                if (task == null) {
                    latch = null
                    //Пробуем получить latch (можем и не успеть)
                } else {
                    //Тут !!, чтобы могло упасть в случае реальной ошибки
                    latch = taskWaiters[task]!!
                }
            } else {
                //Если текущий приоритет выше, чем в ожидающей таске - повышаем пока не поздно
                if (priority.value > task.priority.value) {
                    flowExecutor.toHighPriority(task)
                }
                //Тут !!, чтобы могло упасть в случае реальной ошибки
                latch = taskWaiters[task]!!
            }

            //Если так и не получили latch - Задача новая.
            //Создаем новую таску и все остальное и отправляем поток ждать резульат
            if (latch == null) {
                latch = TaskWaiter()
                task = SuperDataTask(data, priority,
                        Runnable {
                            val result = mainFun.invoke(data)
                            doneTasks[data] = result
                            latch.done()
                        })
                log.info("SUBMITTING NEW TASK: {}", task)
                taskWaiters[task] = latch
                latchSubscribers[task] = 1
                waits[data] = task
                flowExecutor.submit(task)
            } else {
                log.info("EXISTING TASK FOUND!!!")
                latchSubscribers.computeIfPresent(task!!) { _, count -> count + 1 }
            }

            lock.unlock()

            log.debug("AWAITING RESULT...")
            latch.awaitTask()
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
                taskWaiters.remove(task)
            }
            log.debug("DONE!!! RESULT: {}", result)
            return result!!
        } finally {
            //Если оставить здесь доп. проверку на снятие блокировки (например в случае исключений), то возникают не оч понятные дедлоки
//            if (lock.isLocked)
//                lock.unlock()
            printStat("FINALLY")
        }
    }

    private fun printStat(prefix: String) {
        log.debug("\n $prefix STATS: " +
                "\n waits: ${waits.size} " +
                "\n executing: ${executing.size} " +
                "\n latches: ${taskWaiters.size}" +
                "\n done: ${doneTasks.size}")
    }

    private fun aggregateToOneListFuture(futures: List<CompletableFuture<SuperData>>): CompletableFuture<List<SuperData>> {
        return CompletableFuture
                .allOf(*futures.toTypedArray())
                .thenApply { futures.asSequence().map { f -> f.join() }.toList() }
    }
}