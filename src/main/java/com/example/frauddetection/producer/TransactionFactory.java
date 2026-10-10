package com.example.frauddetection.producer;

import com.example.frauddetection.avro.Transaction;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public final class TransactionFactory {

    private static final String[] MERCHANTS = {"Zalando", "Bol.com", "Carrefour", "IKEA", "Amazon.de", "Spotify"};
    private static final String[] COUNTRIES = {"DE", "NL", "FR", "ES", "IT", "SE"};

    private TransactionFactory() {
    }

    public static Transaction create(String transactionId, String cardId, String merchant,
                                     String country, long amountCents) {
        return Transaction.newBuilder()
                .setTransactionId(transactionId)
                .setCardId(cardId)
                .setMerchant(merchant == null || merchant.isBlank() ? "Unknown" : merchant)
                .setCountry(country == null || country.isBlank() ? "NL" : country)
                .setAmountCents(amountCents)
                .setCurrency("EUR")
                .setTimestamp(System.currentTimeMillis())
                .build();
    }

    public static Transaction random(String cardId, long amountCents) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        return create(UUID.randomUUID().toString(), cardId,
                MERCHANTS[r.nextInt(MERCHANTS.length)],
                COUNTRIES[r.nextInt(COUNTRIES.length)],
                amountCents);
    }
}
