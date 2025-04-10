package cdc.wallet.management.service;

import cdc.wallet.management.dto.KafkaMessageDTO;
import cdc.wallet.management.model.WalletMaster;
import cdc.wallet.management.repository.WalletMasterRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class KafkaConsumerService {

    private final WalletMasterService walletMasterService;

    private final WalletMasterRepository walletMasterRepository;

    private final WalletTransactionService walletTransactionService;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

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
                        kafkaMessageDTO.getWalletMasterId(), "Reversal");
            else
                walletMasterService.updateWalletMaster(walletMaster, kafkaMessageDTO.getTransactionAmount());
        } catch (Exception e) {
            logger.error("Exception while consuming the topic:{}", e.getMessage());
        }
    }

}
