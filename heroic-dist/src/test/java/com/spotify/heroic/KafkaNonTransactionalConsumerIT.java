package com.spotify.heroic;

public class KafkaNonTransactionalConsumerIT extends AbstractKafkaConsumerIT {

    @Override
    boolean useTransactionalConsumer() {
        return false;
    }
}
