package cdc.wallet.management.service;

import cdc.wallet.management.model.WalletTransaction;
import cdc.wallet.management.repository.WalletTransactionRepository;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class WalletTransactionService {

    private final WalletTransactionRepository walletTransactionRepository;

    private static final String CREDIT = "CREDIT";

    private static final String DEBIT = "DEBIT";

    public WalletTransactionService(WalletTransactionRepository walletTransactionRepository) {
        this.walletTransactionRepository = walletTransactionRepository;
    }

    public void addTransaction(@NotNull BigDecimal amount,
                               @NotNull Long walletMasterId, @NotBlank String description) {
        walletTransactionRepository.save(new WalletTransaction(
                walletMasterId, amount.signum() > 0 ? CREDIT : DEBIT, amount, description));
    }
}
