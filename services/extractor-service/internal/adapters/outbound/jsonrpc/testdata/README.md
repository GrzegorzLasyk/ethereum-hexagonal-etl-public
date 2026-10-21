# Ethereum RPC result fixtures

These synthetic, internally consistent JSON-RPC **results** model a mined Ethereum
Mainnet block with a dynamic-fee transaction, a failed legacy contract creation,
two receipts, and one event log. They are not real signed transactions or a
cryptographically valid block. Tests add the JSON-RPC envelope and request ID.

`block.json` includes full transaction objects and modern header fields.
`receipt_0.json` and `receipt_1.json` are separate results, in transaction order.
Unknown extension fields exercise byte-preserving acquisition. Tests mutate these
fixtures to cover historical receipts, empty blocks, transaction types 1/3/4,
missing data, malformed quantities, and conflicting identity metadata.

No provider key, live response, or private account data is included.
