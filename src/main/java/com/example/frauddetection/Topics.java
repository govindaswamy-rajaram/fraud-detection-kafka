package com.example.frauddetection;

public final class Topics {

    /** Card transactions. Key = cardId, value = Avro Transaction. */
    public static final String TRANSACTIONS = "transactions";

    /** Alerts produced by the Kafka Streams topology. Key = cardId, value = Avro FraudAlert. */
    public static final String FRAUD_ALERTS = "fraud-alerts";

    /** Dead letter topics: "<original topic>.DLT". */
    public static final String TRANSACTIONS_DLT = TRANSACTIONS + ".DLT";
    public static final String FRAUD_ALERTS_DLT = FRAUD_ALERTS + ".DLT";

    private Topics() {
    }
}
