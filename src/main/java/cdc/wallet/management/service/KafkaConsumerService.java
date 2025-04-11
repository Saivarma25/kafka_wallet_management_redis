package cdc.wallet.management.service;

import cdc.wallet.management.dto.KafkaMessageDTO;
import cdc.wallet.management.model.WalletMaster;
import cdc.wallet.management.repository.WalletMasterRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class KafkaConsumerService {

    private final WalletMasterService walletMasterService;

    private final WalletMasterRepository walletMasterRepository;

    private final WalletTransactionService walletTransactionService;

    private static final String WALLET_TOPIC = "wallet_topic";

    public KafkaConsumerService(WalletMasterService walletMasterService,
                                WalletMasterRepository walletMasterRepository,
                                WalletTransactionService walletTransactionService) {
        this.walletMasterService = walletMasterService;
        this.walletMasterRepository = walletMasterRepository;
        this.walletTransactionService = walletTransactionService;
    }

    @KafkaListener(topics = WALLET_TOPIC)
    @Transactional
    public void processWalletMessage(KafkaMessageDTO kafkaMessageDTO) {
        try {
            WalletMaster walletMaster = walletMasterRepository.findByWalletMasterId(kafkaMessageDTO.getWalletMasterId());

            walletTransactionService.addTransaction(kafkaMessageDTO.getTransactionAmount(),
                    walletMaster.getWalletMasterId(), kafkaMessageDTO.getDescription());
            if (kafkaMessageDTO.getTotalAmount().signum() < 0)
                walletTransactionService.addTransaction(kafkaMessageDTO.getTransactionAmount().abs(),
                        kafkaMessageDTO.getWalletMasterId(), "Reversal of " + kafkaMessageDTO.getDescription());
            else
                walletMasterService.updateWalletMaster(walletMaster, kafkaMessageDTO.getTransactionAmount());
        } catch (Exception e) {
            log.error("Exception while consuming the topic:{}", e.getMessage());
        }
    }

}
