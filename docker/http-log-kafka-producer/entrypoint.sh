#!/bin/sh
java -jar /home/httploggen-1.0.0-jar-with-dependencies.jar --bootstrap ${KAFKA_BOOTSTRAP_SERVERS} --delay ${KAFKA_PRODUCER_DELAY_MS}
