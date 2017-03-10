package com.spotify.heroic;

public class KafkaTransactionalConsumerIT extends AbstractKafkaConsumerIT {

    @Override
    boolean useTransactionalConsumer() {
        return true;
    }
}
