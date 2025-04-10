package cdc.wallet.management.dto;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;

/**
 * DTO to share basic details of the wallet and transaction
 */
@Data
public class WalletDTO implements Serializable {

    @Serial
    private static final long serialVersionUID = 5109640466212908228L;

    private Long walletMasterId;

    private Long clientId;

    private String currency;

    private BigDecimal amount;

    private String description;

}
