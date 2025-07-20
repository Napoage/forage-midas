//Midas Core needs a way to receive all incoming transactions. 
//   To this end, you must implement a class that listens to a Kafka topic and handles incoming messages. 
//    The name of the topic in question has already been added as a configurable value in the project's application.yml file. 
//   The Kafka Listener you implement should use this configuration value to select its topic. 
//    Your Kafka Listener should deserialize all incoming messages to the provided transaction class. 
//    Your goal for this task is simply to integrate Kafka into Midas Core, no need to do anything with the transactions yet, 
//    that comes later. The provided tests utilize an in-memory, embedded kafka instance which should autowire itself to your
//    Spring Application, so there is no need to specify a host or port in your consumer configuration.
//Once you are successfully receiving transactions, execute “TaskTwoTests” in the test folder of the repo and use your debugger
// to record the amount attached to the first four transactions received by Midas Core. Once you have them noted down,
// submit the list below.
package com.jpmc.midascore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.stereotype.Component;
import org.springframework.kafka.annotation.KafkaListener;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "my-group")
    public void listen(Transaction transaction) {
        logger.info("Transaction amount: {}", transaction.getAmount());
    }
}
