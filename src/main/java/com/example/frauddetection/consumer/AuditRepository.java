package com.example.frauddetection.consumer;

import com.example.frauddetection.avro.Transaction;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class AuditRepository {

    private final JdbcTemplate jdbc;

    public AuditRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Idempotent write: the primary key on transaction_id rejects a second insert.
     * Kafka delivers at-least-once, so the same event can arrive twice (retry, rebalance, producer
     * resend). Making the handler idempotent turns "at-least-once" into "effectively once".
     *
     * @return true if the row was new, false if it was a duplicate
     */
    public boolean saveIfNew(Transaction tx) {
        try {
            jdbc.update("INSERT INTO audit_log (transaction_id, card_id, merchant, country, amount_cents, currency, event_ts) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    tx.getTransactionId(), tx.getCardId(), tx.getMerchant(), tx.getCountry(),
                    tx.getAmountCents(), tx.getCurrency(), tx.getTimestamp());
            return true;
        } catch (DuplicateKeyException duplicate) {
            return false;
        }
    }

    public long count() {
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM audit_log", Long.class);
        return count == null ? 0 : count;
    }
}
