package com.example.frauddetection.api;

import com.example.frauddetection.avro.Transaction;
import com.example.frauddetection.consumer.AuditRepository;
import com.example.frauddetection.consumer.DemoState;
import com.example.frauddetection.producer.TransactionFactory;
import com.example.frauddetection.producer.TransactionProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/** Small REST surface to trigger every scenario and look at the results. */
@RestController
@RequestMapping("/api")
public class SimulatorController {

    private final TransactionProducer producer;
    private final AuditRepository audit;
    private final DemoState state;

    public SimulatorController(TransactionProducer producer, AuditRepository audit, DemoState state) {
        this.producer = producer;
        this.audit = audit;
        this.state = state;
    }

    public record TransactionRequest(String cardId, String merchant, String country, Long amountCents) {
    }

    @PostMapping("/transactions")
    public ResponseEntity<Map<String, Object>> send(@RequestBody TransactionRequest req) {
        if (req.cardId() == null || req.cardId().isBlank() || req.amountCents() == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "cardId and amountCents are required"));
        }
        Transaction tx = TransactionFactory.create(
                UUID.randomUUID().toString(), req.cardId(), req.merchant(), req.country(), req.amountCents());
        producer.send(tx);
        return ResponseEntity.accepted().body(Map.of("transactionId", tx.getTransactionId(), "cardId", tx.getCardId()));
    }

    /** N transactions on N different cards: no alerts expected. */
    @PostMapping("/simulate/normal")
    public Map<String, Object> normal(@RequestParam(defaultValue = "10") int count) {
        String run = shortId();
        for (int i = 0; i < count; i++) {
            producer.send(TransactionFactory.random("card-normal-" + run + "-" + i,
                    ThreadLocalRandom.current().nextLong(500, 20_000)));
        }
        return Map.of("sent", count, "expectedAlerts", 0);
    }

    /** 8 transactions on one card within seconds: VELOCITY alert expected. */
    @PostMapping("/simulate/velocity")
    public Map<String, Object> velocity(@RequestParam(defaultValue = "8") int count) {
        String cardId = "card-velocity-" + shortId();
        for (int i = 0; i < count; i++) {
            producer.send(TransactionFactory.random(cardId, 1_500));
        }
        return Map.of("cardId", cardId, "sent", count, "expectedAlert", "VELOCITY");
    }

    /** One 25,000 EUR transaction: HIGH_AMOUNT alert expected. */
    @PostMapping("/simulate/high-amount")
    public Map<String, Object> highAmount() {
        String cardId = "card-big-" + shortId();
        producer.send(TransactionFactory.random(cardId, 2_500_000));
        return Map.of("cardId", cardId, "expectedAlert", "HIGH_AMOUNT");
    }

    /** Same transactionId sent twice: the audit table must contain it once (idempotent consumer). */
    @PostMapping("/simulate/duplicate")
    public Map<String, Object> duplicate() {
        Transaction tx = TransactionFactory.random("card-dup-" + shortId(), 4_200);
        producer.send(tx);
        producer.send(tx);
        return Map.of("transactionId", tx.getTransactionId(), "sent", 2, "expectedAuditRows", 1);
    }

    /** Negative amount: permanent business error, goes to the DLT without retries. */
    @PostMapping("/simulate/invalid")
    public Map<String, Object> invalid() {
        Transaction tx = TransactionFactory.random("card-invalid-" + shortId(), -500);
        producer.send(tx);
        return Map.of("transactionId", tx.getTransactionId(), "expected", "dead letter (InvalidTransactionException)");
    }

    /** Bytes that are not Avro: deserialization failure, forwarded unchanged to the DLT. */
    @PostMapping("/simulate/poison")
    public Map<String, Object> poison() {
        return Map.of("key", producer.sendPoison(), "expected", "dead letter (deserialization error)");
    }

    /** A Kafka transaction with several records. fail=true aborts it: consumers must see none of them. */
    @PostMapping("/simulate/atomic-batch")
    public Map<String, Object> atomicBatch(@RequestParam(defaultValue = "false") boolean fail) {
        String run = shortId();
        List<Transaction> batch = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            batch.add(TransactionFactory.random("card-batch-" + run + "-" + i, 3_000));
        }
        try {
            producer.sendAtomically(batch, fail);
            return Map.of("result", "committed", "records", batch.size());
        } catch (IllegalStateException e) {
            return Map.of("result", "aborted", "reason", e.getMessage(), "visibleToConsumers", 0);
        }
    }

    @GetMapping("/alerts")
    public List<String> alerts() {
        return state.alerts();
    }

    @GetMapping("/dlt")
    public List<String> deadLetters() {
        return state.deadLetters();
    }

    @GetMapping("/audit/count")
    public Map<String, Object> auditCount() {
        return Map.of("auditRows", audit.count());
    }

    private static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
