# Transformed JSON v1 fixture

`transformed_v1.json` was produced by the existing Stage 7 Transformer decoder
and JSON document encoder from its block/receipt fixtures. It includes a failed
legacy contract-creation transaction, dynamic-fee transaction and a receipt log.
The first transaction value is set to the maximum uint256 and total difficulty
is explicitly null to exercise exact numeric storage and optional presence.
Provider is `fixture`; extraction time is `1970-01-01T00:01:40.000000123Z`.
It is synthetic data, not a live-provider snapshot or cryptographic proof.
