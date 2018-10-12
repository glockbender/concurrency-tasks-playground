package com.example.springrabbittest

import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SuperDataTask(
        val data: SuperData,
        val priority: DataPriority,
        private val runnable: Runnable,
        private val created: OffsetDateTime = OffsetDateTime.now()
) : Runnable, Comparable<SuperDataTask> {

    override fun compareTo(other: SuperDataTask): Int {
        var result = other.priority.compareTo(this.priority)
        if (result == 0) {
            result = this.created.compareTo(other.created)
        }
        return result
    }

    override fun toString(): String {
        return "SuperDataTask(data=$data)"
    }

    override fun run() {
        runnable.run()
    }

    fun copy(
            data: SuperData = this.data,
            priority: DataPriority = this.priority,
            created: OffsetDateTime = this.created,
            runnable: Runnable = this.runnable
    ) = SuperDataTask(data = data, priority = priority, runnable = runnable, created = created)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SuperDataTask

        if (data != other.data) return false

        return true
    }

    override fun hashCode(): Int {
        return data.hashCode()
    }
}

class SuperDataExecutor(
        private val beforeExecute: (SuperDataTask) -> Unit,
        private val afterExecute: (SuperDataTask) -> Unit
) : ThreadPoolExecutor(0, 2, 30, TimeUnit.SECONDS,
        PriorityBlockingQueue<SuperDataTask>(5) as PriorityBlockingQueue<Runnable>) {

    private val log = LoggerFactory.getLogger(this::class.simpleName)

    private val lock = ReentrantLock()

    override fun beforeExecute(t: Thread?, r: Runnable?) {
        super.beforeExecute(t, r)
        log.debug("Before execute: {}", r)
        beforeExecute.invoke(r as SuperDataTask)
    }

    override fun afterExecute(r: Runnable?, t: Throwable?) {
        super.afterExecute(r, t)
        log.debug("After execute: {}", r)
        afterExecute.invoke(r as SuperDataTask)
    }

    fun submit(task: SuperDataTask) =
            lock.withLock {
                log.info("Submitting task: {}", task)
                log.info("QUEUE SIZE: {}", queue.size)
                execute(task)
                task
            }


    fun toHighPriority(task: SuperDataTask) {
        log.info("Try to high priority task: {}", task)

        if (task.priority == DataPriority.HIGH) return

        lock.withLock {
            return if (queue.remove(task)) {
                log.debug("Old task removed from queue!!!")
                val newTask = task.copy(
                        priority = DataPriority.HIGH
                )
                execute(newTask)
                log.debug("New task executed")
            } else {
                log.warn("Can't remove old task from queue!!!")
            }
        }
    }
}