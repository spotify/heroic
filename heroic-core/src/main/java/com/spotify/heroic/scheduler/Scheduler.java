package com.spotify.heroic.scheduler;

import java.util.concurrent.TimeUnit;

public interface Scheduler {

    /**
     * Schedule a task to be executed periodically.
     *
     * Same as {@link Scheduler#periodically(String, long, TimeUnit, Task)} but with a default name.
     */
    void periodically(long value, TimeUnit unit, Task task);

    /**
     * Schedule a task to be executed periodically.
     *
     * The task will be scheduler to execute immediately once.
     *
     * @param name Name of the task to execute (helpful when troubleshooting).
     * @param value Time interval that the task should execute.
     * @param unit Unit of the time interval.
     * @param task Task to execute.
     */
    void periodically(String name, long value, TimeUnit unit, Task task);

    void schedule(long value, TimeUnit unit, Task task);

    /**
     * Schedule a task to be executed after the given timeout.
     *
     * @param name The name of the task.
     * @param value Time interval that the task should execute.
     * @param unit Unit of the time interval.
     * @param task Task to execute.
     */
    void schedule(String name, long value, TimeUnit unit, Task task);

    /**
     * Stop the scheduler.
     */
    void stop();
}
