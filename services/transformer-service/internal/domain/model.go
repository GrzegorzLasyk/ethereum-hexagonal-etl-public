// model.go implements service-owned normalized Ethereum records and exact optional/value types in
// the pure domain layer.

// Package domain models validated Ethereum data without infrastructure types.
package domain

// Quantity is a canonical nonnegative decimal integer, never a float.
type Quantity string

// ChainID is a canonical nonnegative decimal chain identifier, independent of machine integer
// width.
type ChainID string

// Hash is a fixed 32-byte Ethereum identity; a height alone is not sufficient to distinguish
// forks.
type Hash [32]byte

// Address is a fixed 20-byte Ethereum address; optional recipients represent null separately.
type Address [20]byte

// Bloom is the fixed 256-byte Ethereum log bloom retained in the normalized model.
type Bloom [256]byte

// Nonce is a fixed eight-byte block nonce, distinct from a transaction account nonce.
type Nonce [8]byte

// Optional preserves absent, explicit null and present values independently.
type Optional[T any] struct {
	Present bool
	Null    bool
	Value   T
}

// HasValue distinguishes a present non-null value from both absent and explicitly null fields.
func (o Optional[T]) HasValue() bool { return o.Present && !o.Null }

// Snapshot groups one block with its ordered transactions and receipts; Validate enforces complete
// identity and index relationships.
type Snapshot struct {
	ChainID      ChainID
	Block        Block
	Transactions []Transaction
	Receipts     []Receipt
}

// Block models normalized mined-block fields and explicitly optional fork-era extensions.
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

// Transaction models transaction identity, exact quantities and type-dependent fields without
// transport or SQL types.
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

// Receipt models execution outcome, historical state root and ordered logs for one transaction.
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

// Log models a receipt log with block/transaction position, ordered topics and data.
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

// Access models one access-list address with its ordered storage-key entries.
type Access struct {
	Address     Address
	StorageKeys []Hash
}

// Authorization models the chain/address/nonce and signature tuple of a transaction authorization.
type Authorization struct {
	ChainID Quantity
	Address Address
	Nonce   uint64
	YParity uint64
	R       Quantity
	S       Quantity
}
