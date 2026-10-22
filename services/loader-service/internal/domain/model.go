// Package domain models validated Ethereum data without infrastructure types.
package domain

// Quantity is a canonical nonnegative decimal integer, never a float.
type Quantity string
type ChainID string
type Hash [32]byte
type Address [20]byte
type Bloom [256]byte
type Nonce [8]byte

// Optional preserves absent, explicit null and present values independently.
type Optional[T any] struct {
	Present bool
	Null    bool
	Value   T
}

func (o Optional[T]) HasValue() bool { return o.Present && !o.Null }

type Snapshot struct {
	ChainID      ChainID
	Block        Block
	Transactions []Transaction
	Receipts     []Receipt
}

type Block struct {
	Number                uint64
	Hash                  Hash
	ParentHash            Hash
	Timestamp             uint64
	Nonce                 Nonce
	Sha3Uncles            Hash
	LogsBloom             Bloom
	TransactionsRoot      Hash
	StateRoot             Hash
	ReceiptsRoot          Hash
	Miner                 Address
	Difficulty            Quantity
	ExtraData             []byte
	GasLimit              uint64
	GasUsed               uint64
	MixHash               Hash
	Size                  Quantity
	Uncles                []Hash
	TotalDifficulty       Optional[Quantity]
	BaseFeePerGas         Optional[Quantity]
	WithdrawalsRoot       Optional[Hash]
	BlobGasUsed           Optional[uint64]
	ExcessBlobGas         Optional[uint64]
	ParentBeaconBlockRoot Optional[Hash]
	RequestsHash          Optional[Hash]
}

type Transaction struct {
	Hash                 Hash
	BlockHash            Hash
	BlockNumber          uint64
	Index                uint64
	From                 Address
	To                   Optional[Address]
	Nonce                uint64
	Value                Quantity
	Gas                  uint64
	Input                []byte
	R                    Quantity
	S                    Quantity
	V                    Optional[Quantity]
	YParity              Optional[uint64]
	Type                 Optional[uint64]
	ChainID              Optional[ChainID]
	GasPrice             Optional[Quantity]
	MaxFeePerGas         Optional[Quantity]
	MaxPriorityFeePerGas Optional[Quantity]
	AccessList           Optional[[]Access]
	MaxFeePerBlobGas     Optional[Quantity]
	BlobVersionedHashes  Optional[[]Hash]
	AuthorizationList    Optional[[]Authorization]
}

type Receipt struct {
	TransactionHash   Hash
	Index             uint64
	BlockHash         Hash
	BlockNumber       uint64
	From              Address
	To                Optional[Address]
	ContractAddress   Optional[Address]
	CumulativeGasUsed uint64
	GasUsed           uint64
	LogsBloom         Bloom
	Type              Optional[uint64]
	EffectiveGasPrice Optional[Quantity]
	BlobGasUsed       Optional[uint64]
	BlobGasPrice      Optional[Quantity]
	Status            Optional[uint64]
	Root              Optional[Hash]
	Logs              []Log
}

type Log struct {
	BlockHash        Hash
	BlockNumber      uint64
	TransactionHash  Hash
	TransactionIndex uint64
	Index            uint64
	Address          Address
	Topics           []Hash
	Data             []byte
	Removed          Optional[bool]
	BlockTimestamp   Optional[uint64]
}

type Access struct {
	Address     Address
	StorageKeys []Hash
}

type Authorization struct {
	ChainID Quantity
	Address Address
	Nonce   uint64
	YParity uint64
	R       Quantity
	S       Quantity
}
