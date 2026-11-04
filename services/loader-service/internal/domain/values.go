// values.go implements field representation and presence/null invariants in the pure domain layer.

package domain

// validateValues validates field representations and optional-value coherence before cross-record
// identity/membership rules run.
func (s Snapshot) validateValues() error {
	if err := s.Block.validateValues(); err != nil {
		return err
	}
	for _, t := range s.Transactions {
		if err := t.validateValues(); err != nil {
			return err
		}
	}
	for _, r := range s.Receipts {
		if err := r.validateValues(); err != nil {
			return err
		}
		for _, l := range r.Logs {
			if err := l.validateValues(); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateValues validates field representations and optional-value coherence before cross-record
// identity/membership rules run.
func (v Block) validateValues() error {
	if !v.Difficulty.Valid() {
		return Invalid("Block.Difficulty")
	}
	if !v.Size.Valid() {
		return Invalid("Block.Size")
	}
	if v.Uncles == nil {
		return Invalid("Block.Uncles")
	}
	if v.TotalDifficulty.HasValue() && !v.TotalDifficulty.Value.Valid() {
		return Invalid("Block.TotalDifficulty")
	}
	if v.TotalDifficulty.Null && !v.TotalDifficulty.Present {
		return Invalid("Block.TotalDifficulty presence")
	}
	if v.BaseFeePerGas.HasValue() && !v.BaseFeePerGas.Value.Valid() {
		return Invalid("Block.BaseFeePerGas")
	}
	if v.BaseFeePerGas.Null && !v.BaseFeePerGas.Present {
		return Invalid("Block.BaseFeePerGas presence")
	}
	if v.WithdrawalsRoot.Null && !v.WithdrawalsRoot.Present {
		return Invalid("Block.WithdrawalsRoot presence")
	}
	if v.BlobGasUsed.Null && !v.BlobGasUsed.Present {
		return Invalid("Block.BlobGasUsed presence")
	}
	if v.ExcessBlobGas.Null && !v.ExcessBlobGas.Present {
		return Invalid("Block.ExcessBlobGas presence")
	}
	if v.ParentBeaconBlockRoot.Null && !v.ParentBeaconBlockRoot.Present {
		return Invalid("Block.ParentBeaconBlockRoot presence")
	}
	if v.RequestsHash.Null && !v.RequestsHash.Present {
		return Invalid("Block.RequestsHash presence")
	}
	return nil
}

// validateValues validates field representations and optional-value coherence before cross-record
// identity/membership rules run.
func (v Transaction) validateValues() error {
	if v.To.Null && !v.To.Present {
		return Invalid("Transaction.To presence")
	}
	if !v.Value.Valid() {
		return Invalid("Transaction.Value")
	}
	if !v.R.Valid() {
		return Invalid("Transaction.R")
	}
	if !v.S.Valid() {
		return Invalid("Transaction.S")
	}
	if v.V.HasValue() && !v.V.Value.Valid() {
		return Invalid("Transaction.V")
	}
	if v.V.Null && !v.V.Present {
		return Invalid("Transaction.V presence")
	}
	if v.YParity.Null && !v.YParity.Present {
		return Invalid("Transaction.YParity presence")
	}
	if v.Type.Null && !v.Type.Present {
		return Invalid("Transaction.Type presence")
	}
	if v.ChainID.HasValue() && !v.ChainID.Value.Valid() {
		return Invalid("Transaction.ChainID")
	}
	if v.ChainID.Null && !v.ChainID.Present {
		return Invalid("Transaction.ChainID presence")
	}
	if v.GasPrice.HasValue() && !v.GasPrice.Value.Valid() {
		return Invalid("Transaction.GasPrice")
	}
	if v.GasPrice.Null && !v.GasPrice.Present {
		return Invalid("Transaction.GasPrice presence")
	}
	if v.MaxFeePerGas.HasValue() && !v.MaxFeePerGas.Value.Valid() {
		return Invalid("Transaction.MaxFeePerGas")
	}
	if v.MaxFeePerGas.Null && !v.MaxFeePerGas.Present {
		return Invalid("Transaction.MaxFeePerGas presence")
	}
	if v.MaxPriorityFeePerGas.HasValue() && !v.MaxPriorityFeePerGas.Value.Valid() {
		return Invalid("Transaction.MaxPriorityFeePerGas")
	}
	if v.MaxPriorityFeePerGas.Null && !v.MaxPriorityFeePerGas.Present {
		return Invalid("Transaction.MaxPriorityFeePerGas presence")
	}
	if v.AccessList.HasValue() && v.AccessList.Value == nil {
		return Invalid("Transaction.AccessList")
	}
	if v.AccessList.HasValue() {
		for _, item := range v.AccessList.Value {
			if err := item.validateValues(); err != nil {
				return err
			}
		}
	}
	if v.AccessList.Null && !v.AccessList.Present {
		return Invalid("Transaction.AccessList presence")
	}
	if v.MaxFeePerBlobGas.HasValue() && !v.MaxFeePerBlobGas.Value.Valid() {
		return Invalid("Transaction.MaxFeePerBlobGas")
	}
	if v.MaxFeePerBlobGas.Null && !v.MaxFeePerBlobGas.Present {
		return Invalid("Transaction.MaxFeePerBlobGas presence")
	}
	if v.BlobVersionedHashes.HasValue() && v.BlobVersionedHashes.Value == nil {
		return Invalid("Transaction.BlobVersionedHashes")
	}
	if v.BlobVersionedHashes.Null && !v.BlobVersionedHashes.Present {
		return Invalid("Transaction.BlobVersionedHashes presence")
	}
	if v.AuthorizationList.HasValue() && v.AuthorizationList.Value == nil {
		return Invalid("Transaction.AuthorizationList")
	}
	if v.AuthorizationList.HasValue() {
		for _, item := range v.AuthorizationList.Value {
			if err := item.validateValues(); err != nil {
				return err
			}
		}
	}
	if v.AuthorizationList.Null && !v.AuthorizationList.Present {
		return Invalid("Transaction.AuthorizationList presence")
	}
	return nil
}

// validateValues validates field representations and optional-value coherence before cross-record
// identity/membership rules run.
func (v Receipt) validateValues() error {
	if v.To.Null && !v.To.Present {
		return Invalid("Receipt.To presence")
	}
	if v.ContractAddress.Null && !v.ContractAddress.Present {
		return Invalid("Receipt.ContractAddress presence")
	}
	if v.Type.Null && !v.Type.Present {
		return Invalid("Receipt.Type presence")
	}
	if v.EffectiveGasPrice.HasValue() && !v.EffectiveGasPrice.Value.Valid() {
		return Invalid("Receipt.EffectiveGasPrice")
	}
	if v.EffectiveGasPrice.Null && !v.EffectiveGasPrice.Present {
		return Invalid("Receipt.EffectiveGasPrice presence")
	}
	if v.BlobGasUsed.Null && !v.BlobGasUsed.Present {
		return Invalid("Receipt.BlobGasUsed presence")
	}
	if v.BlobGasPrice.HasValue() && !v.BlobGasPrice.Value.Valid() {
		return Invalid("Receipt.BlobGasPrice")
	}
	if v.BlobGasPrice.Null && !v.BlobGasPrice.Present {
		return Invalid("Receipt.BlobGasPrice presence")
	}
	if v.Status.Null && !v.Status.Present {
		return Invalid("Receipt.Status presence")
	}
	if v.Root.Null && !v.Root.Present {
		return Invalid("Receipt.Root presence")
	}
	return nil
}

// validateValues validates field representations and optional-value coherence before cross-record
// identity/membership rules run.
func (v Log) validateValues() error {
	if v.Topics == nil {
		return Invalid("Log.Topics")
	}
	if v.Removed.Null && !v.Removed.Present {
		return Invalid("Log.Removed presence")
	}
	if v.BlockTimestamp.Null && !v.BlockTimestamp.Present {
		return Invalid("Log.BlockTimestamp presence")
	}
	return nil
}

// validateValues validates field representations and optional-value coherence before cross-record
// identity/membership rules run.
func (v Access) validateValues() error {
	if v.StorageKeys == nil {
		return Invalid("Access.StorageKeys")
	}
	return nil
}

// validateValues validates field representations and optional-value coherence before cross-record
// identity/membership rules run.
func (v Authorization) validateValues() error {
	if !v.ChainID.Valid() {
		return Invalid("Authorization.ChainID")
	}
	if !v.R.Valid() {
		return Invalid("Authorization.R")
	}
	if !v.S.Valid() {
		return Invalid("Authorization.S")
	}
	if v.YParity > 1 {
		return Invalid("authorization parity")
	}
	return nil
}
