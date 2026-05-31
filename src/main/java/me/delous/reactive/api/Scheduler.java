package me.delous.reactive.api;

public interface Scheduler {
    void execute(Runnable task);
}
