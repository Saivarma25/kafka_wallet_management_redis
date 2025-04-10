package cdc.wallet.management.service;

import cdc.wallet.management.dto.KafkaMessageDTO;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final KafkaTemplate<Long, KafkaMessageDTO> kafkaTemplate;

    private static final String WALLET_TOPIC = "wallet_topic";

    public KafkaProducerService(KafkaTemplate<Long, KafkaMessageDTO> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // Fire and forget
    public void sendWalletMessage(KafkaMessageDTO kafkaMessageDTO) {
        ProducerRecord<Long, KafkaMessageDTO> producerRecord = new ProducerRecord<>(
                WALLET_TOPIC, kafkaMessageDTO.getWalletMasterId(), kafkaMessageDTO);
        CompletableFuture<SendResult<Long, KafkaMessageDTO>> future = kafkaTemplate.send(producerRecord);
        future.whenComplete((result, ex) -> {
            if (ex != null)
                logger.error("Error sending message to Kafka for walletMasterId {}: {}",
                        kafkaMessageDTO.getWalletMasterId(), ex.getMessage());
        });
    }

}
