package postgres

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"strconv"
)

func hashArray(values []domain.Hash) [][]byte {
	result := make([][]byte, 0, len(values))
	for _, value := range values {
		result = append(result, append([]byte(nil), value[:]...))
	}
	return result
}
func optionalValue[T any](value domain.Optional[T], convert func(T) any) any {
	if !value.HasValue() {
		return nil
	}
	return convert(value.Value)
}
func hexData(b []byte) string { return "0x" + hex.EncodeToString(b) }
func accessJSON(values []domain.Access) (string, error) {
	rows := make([]map[string]any, 0, len(values))
	for _, a := range values {
		keys := make([]string, 0, len(a.StorageKeys))
		for _, h := range a.StorageKeys {
			keys = append(keys, hexData(h[:]))
		}
		rows = append(rows, map[string]any{"address": hexData(a.Address[:]), "storage_keys": keys})
	}
	b, err := json.Marshal(rows)
	return string(b), err
}
func authorizationJSON(values []domain.Authorization) (string, error) {
	rows := make([]map[string]any, 0, len(values))
	for _, a := range values {
		rows = append(rows, map[string]any{"chain_id": string(a.ChainID), "address": hexData(a.Address[:]), "nonce": strconv.FormatUint(a.Nonce, 10), "y_parity": strconv.FormatUint(a.YParity, 10), "r": string(a.R), "s": string(a.S)})
	}
	b, err := json.Marshal(rows)
	return string(b), err
}

func insertBlock(ctx context.Context, tx pgx.Tx, chain domain.ChainID, v domain.Block, eventID string) error {

	present := []string{}
	if v.TotalDifficulty.Present {
		present = append(present, "total_difficulty")
	}
	if v.BaseFeePerGas.Present {
		present = append(present, "base_fee_per_gas")
	}
	if v.WithdrawalsRoot.Present {
		present = append(present, "withdrawals_root")
	}
	if v.BlobGasUsed.Present {
		present = append(present, "blob_gas_used")
	}
	if v.ExcessBlobGas.Present {
		present = append(present, "excess_blob_gas")
	}
	if v.ParentBeaconBlockRoot.Present {
		present = append(present, "parent_beacon_block_root")
	}
	if v.RequestsHash.Present {
		present = append(present, "requests_hash")
	}
	_, err := tx.Exec(ctx, `INSERT INTO loader.blocks (chain_id,block_number,block_hash,parent_hash,timestamp,nonce,sha3_uncles,logs_bloom,transactions_root,state_root,receipts_root,miner,difficulty,extra_data,gas_limit,gas_used,mix_hash,size,uncles,total_difficulty,base_fee_per_gas,withdrawals_root,blob_gas_used,excess_blob_gas,parent_beacon_block_root,requests_hash,present_fields,event_id) VALUES ($1,$2::numeric,$3,$4,$5::numeric,$6,$7,$8,$9,$10,$11,$12,$13::numeric,$14,$15::numeric,$16::numeric,$17,$18::numeric,$19,$20::numeric,$21::numeric,$22,$23::numeric,$24::numeric,$25,$26,$27,$28)`,
		string(chain),
		strconv.FormatUint(v.Number, 10),
		v.Hash[:],
		v.ParentHash[:],
		strconv.FormatUint(v.Timestamp, 10),
		v.Nonce[:],
		v.Sha3Uncles[:],
		v.LogsBloom[:],
		v.TransactionsRoot[:],
		v.StateRoot[:],
		v.ReceiptsRoot[:],
		v.Miner[:],
		string(v.Difficulty),
		v.ExtraData,
		strconv.FormatUint(v.GasLimit, 10),
		strconv.FormatUint(v.GasUsed, 10),
		v.MixHash[:],
		string(v.Size),
		hashArray(v.Uncles),
		optionalValue(v.TotalDifficulty, func(x domain.Quantity) any { return string(x) }),
		optionalValue(v.BaseFeePerGas, func(x domain.Quantity) any { return string(x) }),
		optionalValue(v.WithdrawalsRoot, func(x domain.Hash) any { return x[:] }),
		optionalValue(v.BlobGasUsed, func(x uint64) any { return strconv.FormatUint(x, 10) }),
		optionalValue(v.ExcessBlobGas, func(x uint64) any { return strconv.FormatUint(x, 10) }),
		optionalValue(v.ParentBeaconBlockRoot, func(x domain.Hash) any { return x[:] }),
		optionalValue(v.RequestsHash, func(x domain.Hash) any { return x[:] }),
		present,
		eventID)
	return dbErrorOrNil(err)
}

func insertTransaction(ctx context.Context, tx pgx.Tx, chain domain.ChainID, v domain.Transaction) error {

	present := []string{}
	if v.To.Present {
		present = append(present, "to")
	}
	if v.V.Present {
		present = append(present, "v")
	}
	if v.YParity.Present {
		present = append(present, "y_parity")
	}
	if v.Type.Present {
		present = append(present, "type")
	}
	if v.ChainID.Present {
		present = append(present, "chain_id")
	}
	if v.GasPrice.Present {
		present = append(present, "gas_price")
	}
	if v.MaxFeePerGas.Present {
		present = append(present, "max_fee_per_gas")
	}
	if v.MaxPriorityFeePerGas.Present {
		present = append(present, "max_priority_fee_per_gas")
	}
	if v.AccessList.Present {
		present = append(present, "access_list")
	}
	var AccessListValue any
	if v.AccessList.HasValue() {
		encoded, err := accessJSON(v.AccessList.Value)
		if err != nil {
			return domain.Invalid("access_list encoding")
		}
		AccessListValue = encoded
	}
	if v.MaxFeePerBlobGas.Present {
		present = append(present, "max_fee_per_blob_gas")
	}
	if v.BlobVersionedHashes.Present {
		present = append(present, "blob_versioned_hashes")
	}
	if v.AuthorizationList.Present {
		present = append(present, "authorization_list")
	}
	var AuthorizationListValue any
	if v.AuthorizationList.HasValue() {
		encoded, err := authorizationJSON(v.AuthorizationList.Value)
		if err != nil {
			return domain.Invalid("authorization_list encoding")
		}
		AuthorizationListValue = encoded
	}
	_, err := tx.Exec(ctx, `INSERT INTO loader.transactions (chain_id,transaction_hash,block_hash,block_number,transaction_index,from_address,to_address,nonce,value,gas,input_data,r,s,v,y_parity,type,signature_chain_id,gas_price,max_fee_per_gas,max_priority_fee_per_gas,access_list,max_fee_per_blob_gas,blob_versioned_hashes,authorization_list,present_fields) VALUES ($1,$2,$3,$4::numeric,$5::numeric,$6,$7,$8::numeric,$9::numeric,$10::numeric,$11,$12::numeric,$13::numeric,$14::numeric,$15::numeric,$16::numeric,$17,$18::numeric,$19::numeric,$20::numeric,$21::jsonb,$22::numeric,$23,$24::jsonb,$25)`,
		string(chain),
		v.Hash[:],
		v.BlockHash[:],
		strconv.FormatUint(v.BlockNumber, 10),
		strconv.FormatUint(v.Index, 10),
		v.From[:],
		optionalValue(v.To, func(x domain.Address) any { return x[:] }),
		strconv.FormatUint(v.Nonce, 10),
		string(v.Value),
		strconv.FormatUint(v.Gas, 10),
		v.Input,
		string(v.R),
		string(v.S),
		optionalValue(v.V, func(x domain.Quantity) any { return string(x) }),
		optionalValue(v.YParity, func(x uint64) any { return strconv.FormatUint(x, 10) }),
		optionalValue(v.Type, func(x uint64) any { return strconv.FormatUint(x, 10) }),
		optionalValue(v.ChainID, func(x domain.ChainID) any { return string(x) }),
		optionalValue(v.GasPrice, func(x domain.Quantity) any { return string(x) }),
		optionalValue(v.MaxFeePerGas, func(x domain.Quantity) any { return string(x) }),
		optionalValue(v.MaxPriorityFeePerGas, func(x domain.Quantity) any { return string(x) }),
		AccessListValue,
		optionalValue(v.MaxFeePerBlobGas, func(x domain.Quantity) any { return string(x) }),
		optionalValue(v.BlobVersionedHashes, func(x []domain.Hash) any { return hashArray(x) }),
		AuthorizationListValue,
		present)
	return dbErrorOrNil(err)
}

func insertReceipt(ctx context.Context, tx pgx.Tx, chain domain.ChainID, v domain.Receipt) error {

	present := []string{}
	if v.To.Present {
		present = append(present, "to")
	}
	if v.ContractAddress.Present {
		present = append(present, "contract_address")
	}
	if v.Type.Present {
		present = append(present, "type")
	}
	if v.EffectiveGasPrice.Present {
		present = append(present, "effective_gas_price")
	}
	if v.BlobGasUsed.Present {
		present = append(present, "blob_gas_used")
	}
	if v.BlobGasPrice.Present {
		present = append(present, "blob_gas_price")
	}
	if v.Status.Present {
		present = append(present, "status")
	}
	if v.Root.Present {
		present = append(present, "root")
	}
	_, err := tx.Exec(ctx, `INSERT INTO loader.receipts (chain_id,transaction_hash,transaction_index,block_hash,block_number,from_address,to_address,contract_address,cumulative_gas_used,gas_used,logs_bloom,type,effective_gas_price,blob_gas_used,blob_gas_price,status,root,present_fields) VALUES ($1,$2,$3::numeric,$4,$5::numeric,$6,$7,$8,$9::numeric,$10::numeric,$11,$12::numeric,$13::numeric,$14::numeric,$15::numeric,$16::numeric,$17,$18)`,
		string(chain),
		v.TransactionHash[:],
		strconv.FormatUint(v.Index, 10),
		v.BlockHash[:],
		strconv.FormatUint(v.BlockNumber, 10),
		v.From[:],
		optionalValue(v.To, func(x domain.Address) any { return x[:] }),
		optionalValue(v.ContractAddress, func(x domain.Address) any { return x[:] }),
		strconv.FormatUint(v.CumulativeGasUsed, 10),
		strconv.FormatUint(v.GasUsed, 10),
		v.LogsBloom[:],
		optionalValue(v.Type, func(x uint64) any { return strconv.FormatUint(x, 10) }),
		optionalValue(v.EffectiveGasPrice, func(x domain.Quantity) any { return string(x) }),
		optionalValue(v.BlobGasUsed, func(x uint64) any { return strconv.FormatUint(x, 10) }),
		optionalValue(v.BlobGasPrice, func(x domain.Quantity) any { return string(x) }),
		optionalValue(v.Status, func(x uint64) any { return strconv.FormatUint(x, 10) }),
		optionalValue(v.Root, func(x domain.Hash) any { return x[:] }),
		present)
	return dbErrorOrNil(err)
}

func insertLog(ctx context.Context, tx pgx.Tx, chain domain.ChainID, v domain.Log) error {

	present := []string{}
	if v.Removed.Present {
		present = append(present, "removed")
	}
	if v.BlockTimestamp.Present {
		present = append(present, "block_timestamp")
	}
	_, err := tx.Exec(ctx, `INSERT INTO loader.logs (chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,address,topics,data,removed,block_timestamp,present_fields) VALUES ($1,$2,$3::numeric,$4,$5::numeric,$6::numeric,$7,$8,$9,$10,$11::numeric,$12)`,
		string(chain),
		v.BlockHash[:],
		strconv.FormatUint(v.BlockNumber, 10),
		v.TransactionHash[:],
		strconv.FormatUint(v.TransactionIndex, 10),
		strconv.FormatUint(v.Index, 10),
		v.Address[:],
		hashArray(v.Topics),
		v.Data,
		optionalValue(v.Removed, func(x bool) any { return x }),
		optionalValue(v.BlockTimestamp, func(x uint64) any { return strconv.FormatUint(x, 10) }),
		present)
	return dbErrorOrNil(err)
}

func dbErrorOrNil(err error) error {
	if err == nil {
		return nil
	}
	return dbError(err)
}
