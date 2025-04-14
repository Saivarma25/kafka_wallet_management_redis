package cdc.wallet.management.service;

import cdc.wallet.management.dto.KafkaMessageDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class KafkaProducerService {

    private final KafkaTemplate<Long, KafkaMessageDTO> kafkaTemplate;

    private static final String WALLET_TOPIC = "wallet_topic";

    public KafkaProducerService(KafkaTemplate<Long, KafkaMessageDTO> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Method to send transaction details to kafka which pushes in fire and forgot approach
     * @param kafkaMessageDTO DTO that contains basic details to push a transaction to consumer
     */
    public void sendWalletMessage(KafkaMessageDTO kafkaMessageDTO) {
        ProducerRecord<Long, KafkaMessageDTO> producerRecord = new ProducerRecord<>(
                WALLET_TOPIC, kafkaMessageDTO.getWalletMasterId(), kafkaMessageDTO);
        CompletableFuture<SendResult<Long, KafkaMessageDTO>> future = kafkaTemplate.send(producerRecord);
        future.whenComplete((result, ex) -> {
            if (ex != null)
                log.error("Error sending message to Kafka for walletMasterId {}: {}",
                        kafkaMessageDTO.getWalletMasterId(), ex.getMessage());
            // TODO Retry mechanism for failed transactions
        });
    }

}
