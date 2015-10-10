package com.dch.app.files;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Created by dcherdyntsev on 21.07.2015.
 */
public class ConsumeTimer {

    private long everyTime;
    private volatile Consumer<Long> consumer;
    private volatile long lastTime;
    private ReentrantLock lock = new ReentrantLock();

    public ConsumeTimer(long everyTime, Consumer<Long> consumer) {
        this.everyTime = everyTime;
        this.consumer = consumer;
    }

    public void consumeIfNeed() {
        long currentTime = System.currentTimeMillis();
        if(lastTime + everyTime < System.currentTimeMillis()) {
            lock.lock();
            if(lastTime + everyTime < System.currentTimeMillis()) {
                consumer.accept(currentTime);
                lastTime = currentTime;
            }
            lock.unlock();
        }
    }

    public static ConsumeTimer empty() {
        return new ConsumeTimer(0, null) {
            @Override
            public void consumeIfNeed() {
                /* NOP */
            }
        };
    }

}
