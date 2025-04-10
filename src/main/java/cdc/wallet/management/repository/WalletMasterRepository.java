package cdc.wallet.management.repository;

import cdc.wallet.management.model.WalletMaster;
import jakarta.persistence.LockModeType;
import jakarta.validation.constraints.NotNull;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.stereotype.Repository;

@Repository
public interface WalletMasterRepository extends JpaRepository<WalletMaster, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    WalletMaster findByWalletMasterId(@NotNull Long walletMasterId);

}
