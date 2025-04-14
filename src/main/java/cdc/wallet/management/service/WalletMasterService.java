package cdc.wallet.management.service;

import cdc.wallet.management.dto.KafkaMessageDTO;
import cdc.wallet.management.dto.WalletDTO;
import cdc.wallet.management.model.WalletMaster;
import cdc.wallet.management.repository.WalletMasterRepository;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Objects;

@Service
public class WalletMasterService {

    private final WalletMasterRepository walletMasterRepository;

    private final RedisTemplate<String, BigDecimal> redisTemplate;

    private final KafkaProducerService kafkaProducerService;

    private WalletMasterService self;

    private static final String WALLET = "wallet:";

    public WalletMasterService(WalletMasterRepository walletMasterRepository,
                               RedisTemplate<String, BigDecimal> redisTemplate,
                               KafkaProducerService kafkaProducerService) {
        this.walletMasterRepository = walletMasterRepository;
        this.redisTemplate = redisTemplate;
        this.kafkaProducerService = kafkaProducerService;
    }

    @Autowired
    public void setSelf(@Lazy WalletMasterService walletMasterService) {
        this.self = walletMasterService;
    }

    /**
     * Method to get the wallet balance with the given wallet id
     * @param walletMasterId ID of the wallet to fetch record from cache or db
     * @return WalletMaster object that has wallet details along with balance
     */
    @Transactional
    public WalletMaster getWalletMasterBalance(@NotNull Long walletMasterId) {
        BigDecimal cachedBalance = redisTemplate.opsForValue().get(WALLET + walletMasterId);
        if (cachedBalance != null)
            return new WalletMaster(walletMasterId, cachedBalance);

        WalletMaster walletMaster = walletMasterRepository.findByWalletMasterId(walletMasterId);
        if (walletMaster != null)
            redisTemplate.opsForValue().set(WALLET + walletMaster.getWalletMasterId(), walletMaster.getBalance());
        return walletMaster;
    }

    /**
     * Method to create a wallet with given details
     * @param clientId ID of client to use in wallet creation
     * @param currency Currency that wallet to be created holds
     * @return WalletMaster object which is created with given details
     */
    public WalletMaster createWalletMaster(@NotNull Long clientId, @NotBlank String currency) {
        WalletMaster walletMaster = walletMasterRepository.save(new WalletMaster(
                clientId, currency, new BigDecimal(0), true));
        redisTemplate.opsForValue().set(WALLET + walletMaster.getWalletMasterId(), walletMaster.getBalance());
        return walletMaster;
    }

    /**
     * Method to update wallet balance in DB, which will be called by a virtual thread
     * @param walletMaster existing WalletMaster Object to update
     * @param amountToAdd Amount that needs to be added to the given WalletMaster object
     */
    public void updateWalletMaster(WalletMaster walletMaster, BigDecimal amountToAdd) {
        walletMaster.setBalance(walletMaster.getBalance().add(amountToAdd));
        walletMasterRepository.save(walletMaster);
    }

    /**
     * Method to createATransaction which can be negative or positive
     * @param walletDTO dto that holds basic details to make a transaction
     * @return WalletDTO object that has updated details after transaction
     */
    public WalletDTO createTransactions(@NotNull WalletDTO walletDTO) {
        WalletMaster walletMaster = self.getWalletMasterBalance(walletDTO.getWalletMasterId());
        if (walletMaster == null) return null;

        // Immediately updated redis for thread safety total check
        double newAmount = Objects.requireNonNull(redisTemplate.opsForValue().increment(
                WALLET + walletMaster.getWalletMasterId(), walletDTO.getAmount().doubleValue()));
        if (newAmount < 0)
            // If new total is less than zero revert redis amount which is also thread safe
            redisTemplate.opsForValue().increment(
                    WALLET + walletMaster.getWalletMasterId(), walletDTO.getAmount().negate().doubleValue());

        // Send transaction to kafka(fire and forgot)
        kafkaProducerService.sendWalletMessage(new KafkaMessageDTO(walletDTO.getWalletMasterId(),
                BigDecimal.valueOf(newAmount), walletDTO.getAmount(), walletDTO.getDescription()));
        walletDTO.setAmount(BigDecimal.valueOf(newAmount));
        return walletDTO;
    }

}