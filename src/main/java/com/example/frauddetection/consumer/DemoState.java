package com.example.frauddetection.consumer;

import org.springframework.stereotype.Component;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/** Keeps the most recent alerts and dead letters in memory so the REST API can show them. */
@Component
public class DemoState {

    private static final int MAX_ENTRIES = 50;

    private final Deque<String> alerts = new ConcurrentLinkedDeque<>();
    private final Deque<String> deadLetters = new ConcurrentLinkedDeque<>();

    public void addAlert(String entry) {
        add(alerts, entry);
    }

    public void addDeadLetter(String entry) {
        add(deadLetters, entry);
    }

    public List<String> alerts() {
        return List.copyOf(alerts);
    }

    public List<String> deadLetters() {
        return List.copyOf(deadLetters);
    }

    private static void add(Deque<String> deque, String entry) {
        deque.addFirst(entry);
        while (deque.size() > MAX_ENTRIES) {
            deque.pollLast();
        }
    }
}
