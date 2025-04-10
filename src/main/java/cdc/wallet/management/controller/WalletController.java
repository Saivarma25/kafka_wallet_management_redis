package cdc.wallet.management.controller;

import cdc.wallet.management.dto.WalletDTO;
import cdc.wallet.management.model.WalletMaster;
import cdc.wallet.management.service.WalletMasterService;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@Validated
@RestController
@RequestMapping("/wallet")
public class WalletController {

    private final WalletMasterService walletMasterService;

    public WalletController(WalletMasterService walletMasterService) {
        this.walletMasterService = walletMasterService;
    }

    /**
     * Controller method to get wallet balance by calling getWalletMasterBalance service method
     * @param walletId ID of the wallet to get wallet details
     * @return ResponseEntity<?> which can have wallet details, else an error message
     */
    @GetMapping("/getBalance")
    public ResponseEntity<Object> getWalletBalance(@RequestParam @NotNull Long walletId) {
        WalletMaster walletMaster = walletMasterService.getWalletMasterBalance(walletId);
        if (walletMaster == null)
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No wallet found with given details");
        else
            return ResponseEntity.ok(walletMaster);
    }

    /**
     * Controller method to create a wallet by calling createWalletMaster service method
     * @param walletDTO A dto that holds basic details that are needed to create wallet master
     * @return ResponseEntity<?> which can have wallet created, else an error message
     */
    @PostMapping("/createWallet")
    public ResponseEntity<Object> createWallet(@RequestBody @NotNull WalletDTO walletDTO) {
        WalletMaster walletMaster = walletMasterService.createWalletMaster(walletDTO.getClientId(), walletDTO.getCurrency());
        if (walletMaster == null)
            return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body("Failed to create a wallet");
        else
            return ResponseEntity.ok(walletMaster);
    }

    /**
     * Controller method to add/debit amount to wallet by calling updateWalletAmount service method
     * @param walletDTO A dto that contains basic details to create transaction
     * @return ResponseEntity<?> which can have wallet details after transaction, else an error message
     */
    @PutMapping("/addTransaction")
    public ResponseEntity<Object> addWalletTransaction(@RequestBody @NotNull WalletDTO walletDTO) {
        WalletDTO resultDTO = walletMasterService.createTransactions(walletDTO);
        if (resultDTO == null)
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No wallet found with given details");
        else
            return ResponseEntity.ok(resultDTO);
    }

}
