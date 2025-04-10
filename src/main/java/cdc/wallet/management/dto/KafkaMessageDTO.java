package cdc.wallet.management.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaMessageDTO {

    private Long walletMasterId;

    private BigDecimal totalAmount;

    private BigDecimal transactionAmount;

    private String description;

}
