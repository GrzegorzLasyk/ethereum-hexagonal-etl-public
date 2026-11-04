# Handwritten Go source index

This index covers the single root Go module, including ordinary, integration-tagged,
performance-tagged and separately gated live-test files. Listing a test here does
not mean it ran in Stage 15. Generated protobuf code under `gen/` is excluded.

Read each file introduction for its layer and each declaration comment for its
consistency boundary. Methods below are qualified by receiver; interface methods
are documented in the port itself. Test-only fakes are evidence tools, not runtime
implementations. Paths link to source, avoiding line numbers that drift as comments grow.

283 handwritten files in 65 source directories are indexed. Start with a service's
`cmd/`, then domain, ports, use cases and concrete adapters. Shared `internal/`
packages are technical support; they are not a shared Ethereum domain.

## `internal/adapters/baseline`

- [disabled.go](../../internal/adapters/baseline/disabled.go).
  Types and interfaces: `Probe`.
  Functions and methods: `Configure`, `*Probe.Attach`, `*Probe.Close`.

- [enabled.go](../../internal/adapters/baseline/enabled.go).
  Types and interfaces: `Runtime`, `Result`, `Probe`, `traceKey`, `query`, `batchTraceKey`.
  Functions and methods: `Configure`, `*Probe.Attach`, `*Probe.sample`, `*Probe.Close`, `*Probe.TraceQueryStart`, `*Probe.TraceQueryEnd`, `*Probe.TraceBatchStart`, `*Probe.TraceBatchQuery`, `*Probe.TraceBatchEnd`.

- [enabled_test.go](../../internal/adapters/baseline/enabled_test.go) — tests.
  Functions and methods: `TestProbeOptInAndExclusiveOutput`, `TestBatchProbeCountsWithoutRecordingSQL`.

- [stats.go](../../internal/adapters/baseline/stats.go).
  Types and interfaces: `Distribution`.
  Functions and methods: `*Distribution.Add`, `Distribution.Summary`.
  Package values: `SampleLimit`.

- [stats_test.go](../../internal/adapters/baseline/stats_test.go) — tests.
  Functions and methods: `TestDistribution`.

## `internal/adapters/dbreadiness`

- [check.go](../../internal/adapters/dbreadiness/check.go).
  Types and interfaces: `Database`, `Table`, `Policy`.
  Functions and methods: `Check`, `check`, `nonNil`.
  Package values: `ErrNotReady`.

- [check_test.go](../../internal/adapters/dbreadiness/check_test.go) — tests.
  Types and interfaces: `failingDB`, `successfulDB`.
  Functions and methods: `failingDB.BeginTx`, `TestFailureRedactionAndDeadline`, `*successfulDB.BeginTx`, `TestUsesReadOnlySnapshot`.

## `internal/adapters/etlverify`

- [sql.go](../../internal/adapters/etlverify/sql.go).
  Functions and methods: `sqlFor`.

- [verify.go](../../internal/adapters/etlverify/verify.go).
  Types and interfaces: `Target`, `Query`, `Snapshot`, `Report`.
  Functions and methods: `Target.Validate`, `Observe`, `Report.Complete`, `Wait`, `Report.Summary`, `validStates`.
  Package values: `MainnetBlock`, `ErrIncomplete`, `ErrFailed`, `ErrStuck`, `ErrObservation`.

- [verify_test.go](../../internal/adapters/etlverify/verify_test.go) — tests.
  Functions and methods: `healthy`, `TestBoundedCompletionOffline`, `TestVerifierGuards`, `TestKnownFailureSurvivesDownstreamObservationOutage`.

## `internal/adapters/grpccontract`

- [admission.go](../../internal/adapters/grpccontract/admission.go).
  Types and interfaces: `Resources`, `Admission`, `admissionListener`, `admissionConn`.
  Functions and methods: `DefaultResources`, `Resources.Validate`, `LoadResources`, `Overloaded`, `IsOverloaded`, `NewAdmission`, `*Admission.enter`, `*Admission.ServerOptions`, `*Admission.Register`, `*Admission.wrap`, `*admissionConn.Close`, `*Admission.LimitListener`, `*admissionListener.Accept`.

- [admission_test.go](../../internal/adapters/grpccontract/admission_test.go) — tests.
  Types and interfaces: `fakeAdmissionListener`.
  Functions and methods: `TestAdmissionConcurrentBoundAndCancellation`, `TestAdmissionBeforeDecodeAndAfterHandler`, `TestResourceConfiguration`, `*fakeAdmissionListener.Accept`, `*fakeAdmissionListener.Close`, `*fakeAdmissionListener.Addr`, `TestConnectionBudgetReleasedExactlyOnce`.

- [client.go](../../internal/adapters/grpccontract/client.go).
  Functions and methods: `LocalAddress`, `DurableACK`.

- [limits.go](../../internal/adapters/grpccontract/limits.go).
  Types and interfaces: `Limits`.
  Functions and methods: `DefaultLimits`, `Limits.Validate`, `Limits.ServerOptions`, `Limits.ClientOptions`, `Limits.CheckMessage`, `Limits.CheckPayload`, `payloadTooLarge`, `JSONObject`.
  Package values: `MaxPayloadBytes`, `MaxMessageBytes`.

- [limits_test.go](../../internal/adapters/grpccontract/limits_test.go) — tests.
  Types and interfaces: `sizeServer`.
  Functions and methods: `TestLimitsValidation`, `TestExactPayloadBounds`, `TestEncodedMessageBoundIncludesMetadataAndUnknownFields`, `TestJSONObject`, `*sizeServer.SubmitRawBlock`, `TestGRPCSendReceiveLimits`.

- [metadata.go](../../internal/adapters/grpccontract/metadata.go).
  Functions and methods: `ValidChainID`, `EventID`, `ValidTimestamp`, `ValidateMetadata`, `validHash`.
  Package values: `providerLabel`.

- [metadata_test.go](../../internal/adapters/grpccontract/metadata_test.go) — tests.
  Functions and methods: `metadata`, `TestMetadataValidation`, `TestIdentityBoundaries`.

## `internal/adapters/health`

- [server.go](../../internal/adapters/health/server.go).
  Functions and methods: `Start`, `Handler`.

- [server_test.go](../../internal/adapters/health/server_test.go) — tests.
  Functions and methods: `TestReadinessAndShutdown`, `TestMetricsDoesNotChangeHealth`, `TestReadinessDeadlineAndConcurrentShutdown`, `TestListenerRestrictionsAndClose`.

## `internal/adapters/metrics`

- [admission.go](../../internal/adapters/metrics/admission.go).
  Types and interfaces: `admissionMetrics`.
  Functions and methods: `*Registry.ConfigureAdmission`, `*Registry.AdmissionActive`, `*Registry.AdmissionRejected`, `*Registry.OverloadRetry`, `*Registry.writeAdmission`.

- [admission_test.go](../../internal/adapters/metrics/admission_test.go) — tests.
  Functions and methods: `TestAdmissionMetricsAndFailedSnapshot`.

- [metrics.go](../../internal/adapters/metrics/metrics.go).
  Types and interfaces: `operation`, `Registry`, `Queue`, `Snapshot`.
  Functions and methods: `New`, `*Registry.Observe`, `*Registry.Head`, `*Registry.Cycle`, `family`, `gauge`, `flag`, `*Registry.Handler`, `*Registry.writeOperations`, `*Registry.writeSnapshot`.
  Package values: `bounds`, `outcomes`, `operations`.

- [metrics_test.go](../../internal/adapters/metrics/metrics_test.go) — tests.
  Functions and methods: `scrape`, `TestSnapshotFailureFreshnessAndRecovery`, `TestBoundedLabelsHistogramsConcurrencyAndRestart`, `TestScrapeDeadlineAndConcurrencyBound`.

- [rpc.go](../../internal/adapters/metrics/rpc.go).
  Types and interfaces: `rpcMetrics`.
  Functions and methods: `*Registry.ConfigureRPC`, `*Registry.RPCAttempt`, `*Registry.writeRPC`.

- [rpc_test.go](../../internal/adapters/metrics/rpc_test.go) — tests.
  Functions and methods: `TestRPCBudgetExposition`.

## `internal/logevent`

- [event.go](../../internal/logevent/event.go).
  Types and interfaces: `Event`.
  Functions and methods: `Write`, `ErrorCode`, `FailureOutcome`.
  Package values: `identity`, `decimal`.

- [event_test.go](../../internal/logevent/event_test.go) — tests.
  Types and interfaces: `secretError`.
  Functions and methods: `secretError.Error`, `TestRedactionAndExistingIdentity`.

## `services/extractor-service/cmd/extractor`

- [budget_test.go](../../services/extractor-service/cmd/extractor/budget_test.go) — tests.
  Functions and methods: `TestRunBudget`.

- [lifecycle.go](../../services/extractor-service/cmd/extractor/lifecycle.go).
  Functions and methods: `runAcquisition`.

- [lifecycle_test.go](../../services/extractor-service/cmd/extractor/lifecycle_test.go) — tests.
  Functions and methods: `TestFiniteAcquisitionKeepsDispatchUntilCompletion`, `TestAcquisitionFailureDoesNotClaimDrainSuccess`, `TestFollowHeadGracefulCancellation`.

- [logging_test.go](../../services/extractor-service/cmd/extractor/logging_test.go) — tests.
  Functions and methods: `TestReplayStartupFailureKeepsIdentityWithoutDSN`.

- [main.go](../../services/extractor-service/cmd/extractor/main.go).
  Functions and methods: `main`, `run`, `runWithinBudget`, `runServices`.

## `services/extractor-service/cmd/fixture-capture`

- [main.go](../../services/extractor-service/cmd/fixture-capture/main.go).
  Functions and methods: `main`, `run`.

- [main_test.go](../../services/extractor-service/cmd/fixture-capture/main_test.go) — tests.
  Functions and methods: `TestCaptureRequiresExplicitGateBeforeNetwork`.

## `services/extractor-service/internal/adapters/fixturecapture`

- [artifact.go](../../services/extractor-service/internal/adapters/fixturecapture/artifact.go).
  Types and interfaces: `Artifact`, `Source`, `localTransport`.
  Functions and methods: `digests`, `dimensions`, `Capture`, `containsSecret`, `ValidateFile`, `VerifyFile`, `readVerified`, `readArtifact`, `Artifact.checkSecrets`, `Artifact.Validate`, `localTransport.RoundTrip`, `replay`.

- [config.go](../../services/extractor-service/internal/adapters/fixturecapture/config.go).
  Functions and methods: `Settings`, `SecretTokens`.
  Package values: `ErrConfig`, `endpointPath`.

- [fixture_test.go](../../services/extractor-service/internal/adapters/fixturecapture/fixture_test.go) — tests.
  Types and interfaces: `fakeSource`.
  Functions and methods: `synthetic`, `settings`, `TestSettingsOffline`, `*fakeSource.Requests`, `*fakeSource.Block`, `loadArtifact`, `TestCaptureAndOfflineIntegrity`, `TestSecretsAndCancellationDoNotPublish`, `TestEscapedCredentialEcho`, `TestBoundedAdapterCaptureOffline`, `TestVersionedSelectionManifest`.

- [manifest.go](../../services/extractor-service/internal/adapters/fixturecapture/manifest.go).
  Types and interfaces: `Counts`, `Coverage`, `Candidate`, `Manifest`.
  Functions and methods: `knownFeature`, `Candidate.Validate`, `LoadManifest`, `decode`.
  Package values: `ErrFixture`, `hashPattern`, `idPattern`, `Features`, `integrityChecks`.

- [preflight_test.go](../../services/extractor-service/internal/adapters/fixturecapture/preflight_test.go) — tests.
  Types and interfaces: `racingSource`.
  Functions and methods: `writeArtifact`, `TestPreflightPinsAndBudgets`, `TestVerifyAnchorsSelectionAndNeverUsesNetwork`, `TestOfflineArtifactSecretChecks`, `TestOfflineTransactionReceiptAndLogIntegrity`, `racingSource.Block`, `TestCaptureNeverReplacesExistingOrRacedFiles`.

- [source.go](../../services/extractor-service/internal/adapters/fixturecapture/source.go).
  Types and interfaces: `ReplaySource`.
  Functions and methods: `NewReplaySource`, `*ReplaySource.ChainID`, `*ReplaySource.Block`.

- [source_test.go](../../services/extractor-service/internal/adapters/fixturecapture/source_test.go) — tests.
  Functions and methods: `TestReplaySourceOffline`.

## `services/extractor-service/internal/adapters/inbound/dispatcher`

- [logging_test.go](../../services/extractor-service/internal/adapters/inbound/dispatcher/logging_test.go) — tests.
  Functions and methods: `TestCorrelatedProgressSurvivesCancellationAndErrors`, `TestClaimFailureDoesNotInventIdentity`.

- [metrics_test.go](../../services/extractor-service/internal/adapters/inbound/dispatcher/metrics_test.go) — tests.
  Functions and methods: `TestMetricsIndependentOfLogLevelAndShutdown`.

- [worker.go](../../services/extractor-service/internal/adapters/inbound/dispatcher/worker.go).
  Types and interfaces: `Worker`.
  Functions and methods: `New`, `*Worker.Run`.

- [worker_test.go](../../services/extractor-service/internal/adapters/inbound/dispatcher/worker_test.go) — tests.
  Types and interfaces: `processorFunc`.
  Functions and methods: `processorFunc.DispatchOne`, `await`, `TestBoundedConcurrencyAndJoinedShutdown`, `TestDeadlineIdleAndErrorPolling`, `TestPendingWorkDrainsWithoutPollDelay`.

## `services/extractor-service/internal/adapters/inbound/indexer`

- [logging_test.go](../../services/extractor-service/internal/adapters/inbound/indexer/logging_test.go) — tests.
  Functions and methods: `TestIndexLogsCommittedAndFailedBlocksAtShutdown`, `TestRecoveryLogsOldTipAndAncestorWithoutClaimingSuccess`.

- [worker.go](../../services/extractor-service/internal/adapters/inbound/indexer/worker.go).
  Types and interfaces: `Worker`.
  Functions and methods: `New`, `*Worker.Run`, `writeProgress`.

- [worker_test.go](../../services/extractor-service/internal/adapters/inbound/indexer/worker_test.go) — tests.
  Types and interfaces: `cycleFunc`.
  Functions and methods: `cycleFunc.Cycle`, `TestWorkerBoundedFailureAndRecoveryHalt`, `TestWorkerWaitsForCancellationAndReturnsOnDone`, `TestWorkerBackpressureDoesNotExhaustFailureBudget`.

## `services/extractor-service/internal/adapters/inbound/metrics`

- [handler.go](../../services/extractor-service/internal/adapters/inbound/metrics/handler.go).
  Functions and methods: `Handler`.

## `services/extractor-service/internal/adapters/logging`

- [errors.go](../../services/extractor-service/internal/adapters/logging/errors.go).
  Functions and methods: `ErrorCode`.

## `services/extractor-service/internal/adapters/outbound/grpc`

- [publisher.go](../../services/extractor-service/internal/adapters/outbound/grpc/publisher.go).
  Types and interfaces: `Publisher`.
  Functions and methods: `New`, `*Publisher.Close`, `*Publisher.Publish`.

- [publisher_test.go](../../services/extractor-service/internal/adapters/outbound/grpc/publisher_test.go) — tests.
  Types and interfaces: `recipient`, `publishClientFunc`.
  Functions and methods: `*recipient.SubmitRawBlock`, `publication`, `testPublisher`, `TestPublishVerifiedACKAndExactRetry`, `TestPublishFailures`, `publishClientFunc.SubmitRawBlock`, `TestPublishLocalBudgetsAndCancellation`, `TestPublishCancellationInFlight`, `TestPublishReceiverOverloadIsRetryable`.

## `services/extractor-service/internal/adapters/outbound/jsonrpc`

- [budget_test.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/budget_test.go) — tests.
  Types and interfaces: `budgetTransport`.
  Functions and methods: `TestRequestBudgetCoversBlockReceiptsAndRecoveryHeaders`, `TestBudgetRejectsPartialSnapshot`, `TestBudgetIncludesRetriesAndConcurrentAttempts`, `budgetTransport.RoundTrip`, `TestRunDeadlineCancelsRPCWithoutRetry`.

- [chain.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/chain.go).
  Functions and methods: `*Client.Head`, `*Client.Header`, `*Client.header`.

- [chain_test.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/chain_test.go) — tests.
  Functions and methods: `TestChainObservations`, `TestHeaderRejectsUnavailableOrInvalidIdentity`, `TestHeadRechecksChainAndPropagatesCancellation`.

- [client.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/client.go).
  Types and interfaces: `Config`, `HTTPError`, `RPCError`, `Client`, `rpcRetryKey`, `attemptTransport`.
  Functions and methods: `*HTTPError.Error`, `*RPCError.Error`, `New`, `*Client.ChainID`, `*Client.Block`, `wait`, `*Client.pace`, `*Client.cooldown`, `*Client.call`, `safeTransportError`, `*Client.attempt`, `retryDelay`, `*Client.Requests`, `*Client.reserveRequest`, `attemptTransport.RoundTrip`.
  Package values: `ErrConfig`, `ErrProtocol`, `ErrTransport`, `ErrResponseTooLarge`, `ErrSnapshotTooLarge`.

- [client_test.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/client_test.go) — tests.
  Types and interfaces: `wireRequest`, `fixtureServer`.
  Functions and methods: `testConfig`, `newClient`, `fixture`, `reply`, `serveFixtures`, `TestBlockPreservesSourceAndBuildsMetadata`, `mutate`, `firstTx`, `firstLog`, `TestBlockRejectsPartialOrConflictingData`, `TestEmptyBlockAndHistoricalReceipt`, `TestTypedTransactionVariants`, `TestHTTPRetriesAndCredentialRedaction`, `TestRPCErrorsAndMalformedResponses`, `TestRetryRecoveryAndRetryAfter`, `TestTimeoutAndCancellation`, `TestResponseAndSnapshotBounds`, `TestChainMismatchAndRedirects`, `TestInvalidConfiguration`.
  Package values: `fixtureNumber`.

- [decode.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/decode.go).
  Types and interfaces: `transactionInfo`.
  Functions and methods: `decodeBlock`, `hashArray`, `checkWithdrawals`, `decodeTransaction`, `checkTransactionFields`, `decodeReceipt`, `sameAddress`, `decodeLog`.

- [fields.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/fields.go).
  Types and interfaces: `object`.
  Functions and methods: `invalid`, `objectOf`, `uniqueObject`, `walkJSON`, `parseQuantity`, `uintField`, `object.quantities`, `dataField`, `object.data`, `hashField`, `addressField`, `arrayField`, `blockReference`, `object.optionalQuantities`, `object.optionalData`.

- [live_test.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/live_test.go) — tests.
  Functions and methods: `TestLiveMainnetBlock`.

- [metrics_test.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/metrics_test.go) — tests.
  Functions and methods: `TestRPCMetricsCountActualRetriesAndBudget`.

- [protocol_test.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/protocol_test.go) — tests.
  Types and interfaces: `failingTransport`.
  Functions and methods: `TestInvalidEnvelopes`, `*failingTransport.RoundTrip`, `TestTransportFailureRedaction`, `TestChainChangeDuringSnapshot`, `TestConcurrentRequestsHaveUniqueIDsAndPacing`.

- [receipts.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/receipts.go).
  Functions and methods: `*Client.fetchReceipts`.
  Package values: `DefaultReceiptConcurrency`, `MaxReceiptConcurrency`.

- [receipts_test.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/receipts_test.go) — tests.
  Types and interfaces: `receiptHook`, `batchStore`.
  Functions and methods: `receiptBatch`, `offlineReceiptClient`, `TestConcurrentReceiptOrderingAndRawBytes`, `TestConcurrentReceiptSharedBudget`, `TestConcurrentReceiptCancellationJoinsWorkers`, `TestConcurrentReceiptGlobalPacingAndCooldown`, `TestConcurrentReceiptValidationFailures`, `TestReceiptConcurrencyBounds`, `*batchStore.Checkpoint`, `*batchStore.Commit`, `TestConcurrentReceiptFailureNeverCommits`, `TestConcurrentReceiptCancelPacingAndRetry`.

- [reliability_test.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/reliability_test.go) — tests.
  Types and interfaces: `rpcCycle`, `unreadableBody`.
  Functions and methods: `*rpcCycle.Cycle`, `rpcResponse`, `TestRPCPolicyAcrossWorkerCycles`, `TestRetryAfterTimingAndCycleHalt`, `*unreadableBody.Read`, `*unreadableBody.Close`, `TestPermanentHTTPClassificationDoesNotReadErrorBody`.

- [smoke_config_test.go](../../services/extractor-service/internal/adapters/outbound/jsonrpc/smoke_config_test.go) — tests.
  Functions and methods: `smokeSettings`, `TestLiveSmokeGateOffline`, `TestLiveSmokeExplicitBlockOffline`.

## `services/extractor-service/internal/adapters/outbound/postgres`

- [branch.go](../../services/extractor-service/internal/adapters/outbound/postgres/branch.go).
  Functions and methods: `number`, `*Store.beginBranch`, `commitBranch`, `payloadBytes`, `*Store.observationExists`, `*Store.insertObservation`, `readCapacity`, `*Store.Capacity`, `*Store.checkCapacity`, `*Store.ActiveHeader`, `*Store.Rewind`.
  Package values: `capacitySQL`.

- [drain.go](../../services/extractor-service/internal/adapters/outbound/postgres/drain.go).
  Functions and methods: `*Store.RangeDelivery`.

- [drain_integration_test.go](../../services/extractor-service/internal/adapters/outbound/postgres/drain_integration_test.go) — tests.
  Functions and methods: `testRangeDelivery`.

- [handoff_seed_test.go](../../services/extractor-service/internal/adapters/outbound/postgres/handoff_seed_test.go) — tests.
  Types and interfaces: `fixedProvenanceSource`.
  Functions and methods: `fixedProvenanceSource.Block`, `TestSeedHandoff`.

- [indexing_integration_test.go](../../services/extractor-service/internal/adapters/outbound/postgres/indexing_integration_test.go) — tests.
  Types and interfaces: `indexProvider`.
  Functions and methods: `testIndexing`, `*indexProvider.ChainID`, `*indexProvider.Block`, `*indexProvider.Header`, `*indexProvider.Head`.

- [integration_test.go](../../services/extractor-service/internal/adapters/outbound/postgres/integration_test.go) — tests.
  Types and interfaces: `commitFaultDB`, `commitFaultTx`.
  Functions and methods: `integrationPool`, `TestPostgresIntegration`, `commitFaultDB.BeginTx`, `commitFaultTx.Commit`.

- [mainnet_seed_test.go](../../services/extractor-service/internal/adapters/outbound/postgres/mainnet_seed_test.go) — tests.
  Functions and methods: `TestSeedRealFixture`.

- [metrics_integration_test.go](../../services/extractor-service/internal/adapters/outbound/postgres/metrics_integration_test.go) — tests.
  Functions and methods: `testMetrics`.

- [outbox.go](../../services/extractor-service/internal/adapters/outbound/postgres/outbox.go).
  Types and interfaces: `Outbox`.
  Functions and methods: `NewOutbox`, `deliveryError`, `deliveryRollback`, `deliveryCommit`, `*Outbox.begin`, `*Outbox.ClaimDelivery`, `*Outbox.Published`, `*Outbox.RetryDelivery`.

- [outbox_integration_test.go](../../services/extractor-service/internal/adapters/outbound/postgres/outbox_integration_test.go) — tests.
  Types and interfaces: `deliveryFaultDB`, `deliveryFaultTx`.
  Functions and methods: `deliveryFaultDB.BeginTx`, `deliveryFaultTx.Commit`, `testOutbox`.

- [performance_seed_test.go](../../services/extractor-service/internal/adapters/outbound/postgres/performance_seed_test.go) — tests.
  Types and interfaces: `offlineRoundTripper`.
  Functions and methods: `offlineRoundTripper.RoundTrip`, `syntheticSource`, `TestSeedPerformance`, `TestSyntheticBaselineOfflineSource`.

- [readiness.go](../../services/extractor-service/internal/adapters/outbound/postgres/readiness.go).
  Functions and methods: `*Store.CheckReadiness`.

- [readiness_integration_test.go](../../services/extractor-service/internal/adapters/outbound/postgres/readiness_integration_test.go) — tests.
  Functions and methods: `testReadiness`.

- [readiness_policy.go](../../services/extractor-service/internal/adapters/outbound/postgres/readiness_policy.go).
  Package values: `readinessPolicy`.

- [readiness_policy_test.go](../../services/extractor-service/internal/adapters/outbound/postgres/readiness_policy_test.go) — tests.
  Functions and methods: `TestReadinessMigrationManifest`.

- [reindex.go](../../services/extractor-service/internal/adapters/outbound/postgres/reindex.go).
  Functions and methods: `readReindex`, `*Store.OpenReindex`, `sameID`, `*Store.CommitReindex`, `*Store.Replay`.
  Package values: `reindexSQL`.

- [rpc_reliability_integration_test.go](../../services/extractor-service/internal/adapters/outbound/postgres/rpc_reliability_integration_test.go) — tests.
  Functions and methods: `testRPCCheckpointPreservation`.

- [snapshot.go](../../services/extractor-service/internal/adapters/outbound/postgres/snapshot.go).
  Types and interfaces: `metadataV1`.
  Functions and methods: `encodeMetadata`, `decodeSnapshot`, `*Store.Load`.

- [status.go](../../services/extractor-service/internal/adapters/outbound/postgres/status.go).
  Functions and methods: `*Store.ReadStatus`.

- [store.go](../../services/extractor-service/internal/adapters/outbound/postgres/store.go).
  Types and interfaces: `DatabaseError`, `database`, `Store`.
  Functions and methods: `*DatabaseError.Error`, `*DatabaseError.Unwrap`, `databaseError`, `New`, `readCheckpoint`, `*Store.Checkpoint`, `sameCheckpoint`, `*Store.Commit`.
  Package values: `ErrDatabase`, `ErrCommitUnknown`, `ErrCorrupt`, `ErrNotFound`, `checkpointSQL`.

- [store_test.go](../../services/extractor-service/internal/adapters/outbound/postgres/store_test.go) — tests.
  Functions and methods: `snapshot`, `TestSnapshotRoundTrip`, `TestValidationBeforeDatabaseIO`, `TestDatabaseErrorsAreSafe`.

## `services/extractor-service/internal/application/ports/inbound`

- [dispatch.go](../../services/extractor-service/internal/application/ports/inbound/dispatch.go).
  Types and interfaces: `DispatchResult`, `Dispatcher`.

- [extract_block.go](../../services/extractor-service/internal/application/ports/inbound/extract_block.go).
  Types and interfaces: `ExtractBlockRequest`, `ExtractBlockResult`, `BlockExtractor`.

- [indexing.go](../../services/extractor-service/internal/application/ports/inbound/indexing.go).
  Types and interfaces: `IndexedBlock`, `IndexResult`, `Indexer`.

## `services/extractor-service/internal/application/ports/outbound`

- [chain.go](../../services/extractor-service/internal/application/ports/outbound/chain.go).
  Types and interfaces: `ChainObserver`.
  Package values: `ErrRetryDeferred`, `ErrSourcePermanent`, `ErrRPCBudgetExceeded`, `ErrRunBudgetExceeded`.

- [delivery.go](../../services/extractor-service/internal/application/ports/outbound/delivery.go).
  Types and interfaces: `Publication`, `Delivery`, `DeliveryFailure`, `DeliveryQueue`, `Publisher`.
  Package values: `ErrDeliveryLeaseLost`, `ErrDeliveryCommitUnknown`, `ErrPublishInvalid`, `ErrPublishConflict`, `ErrPublishOversize`, `ErrPublishOverloaded`, `ErrPublishUnavailable`, `ErrPublishACK`.

- [drain.go](../../services/extractor-service/internal/application/ports/outbound/drain.go).
  Types and interfaces: `RangeDelivery`, `RangeDeliveryReader`.
  Package values: `ErrDeliveryIncomplete`, `ErrDeliveryFailed`.

- [extraction.go](../../services/extractor-service/internal/application/ports/outbound/extraction.go).
  Types and interfaces: `BlockSource`, `CommitExtraction`, `CommitResult`, `ExtractionStore`.
  Package values: `ErrCommitUnknown`.

- [indexing.go](../../services/extractor-service/internal/application/ports/outbound/indexing.go).
  Types and interfaces: `BranchStore`, `ReindexRequest`, `ReindexState`, `ReindexStore`, `ReplayRequest`, `ReplayStore`.

- [readiness.go](../../services/extractor-service/internal/application/ports/outbound/readiness.go).
  Types and interfaces: `ReadinessChecker`.

- [status.go](../../services/extractor-service/internal/application/ports/outbound/status.go).
  Types and interfaces: `QueueStatus`, `Status`, `StatusReader`.

## `services/extractor-service/internal/application/usecases`

- [dispatch.go](../../services/extractor-service/internal/application/usecases/dispatch.go).
  Types and interfaces: `Dispatch`.
  Functions and methods: `NewDispatch`, `*Dispatch.DispatchOne`, `*Dispatch.deliveryBackoff`.

- [dispatch_test.go](../../services/extractor-service/internal/application/usecases/dispatch_test.go) — tests.
  Types and interfaces: `deliveryFake`, `publisherFunc`.
  Functions and methods: `*deliveryFake.ClaimDelivery`, `*deliveryFake.Published`, `*deliveryFake.RetryDelivery`, `publisherFunc.Publish`, `TestDispatchAcknowledgementBoundary`, `TestDispatchTimeoutCancellationExhaustionAndIdle`, `TestOverloadExhaustsDurableAllowance`.

- [drain.go](../../services/extractor-service/internal/application/usecases/drain.go).
  Functions and methods: `Drain`.

- [drain_test.go](../../services/extractor-service/internal/application/usecases/drain_test.go) — tests.
  Types and interfaces: `drainReader`.
  Functions and methods: `drainReader.RangeDelivery`, `TestDrain`.

- [extract_block.go](../../services/extractor-service/internal/application/usecases/extract_block.go).
  Types and interfaces: `ExtractBlock`.
  Functions and methods: `NewExtractBlock`, `*ExtractBlock.ExtractBlock`.

- [extract_block_test.go](../../services/extractor-service/internal/application/usecases/extract_block_test.go) — tests.
  Types and interfaces: `fakeSource`, `fakeStore`.
  Functions and methods: `*fakeSource.ChainID`, `*fakeSource.Block`, `*fakeStore.Checkpoint`, `*fakeStore.Commit`, `extractionAt`, `newUseCase`, `request`, `TestExtractBlockErrors`, `TestCancellationBoundaries`, `TestExtractionRecoveryAndIdempotency`, `TestNewExtractBlock`.
  Package values: `testFailure`.

- [index.go](../../services/extractor-service/internal/application/usecases/index.go).
  Types and interfaces: `IndexOptions`, `Index`.
  Functions and methods: `IndexOptions.Validate`, `NewIndex`, `validHeader`, `*Index.Cycle`, `verifyObservation`, `*Index.reconcile`.

- [index_test.go](../../services/extractor-service/internal/application/usecases/index_test.go) — tests.
  Types and interfaces: `branchFixture`, `activeReadFailure`, `exhaustedObserver`.
  Functions and methods: `emptyExtraction`, `newBranch`, `*branchFixture.ChainID`, `*branchFixture.Head`, `*branchFixture.Header`, `*branchFixture.Block`, `*branchFixture.Checkpoint`, `*branchFixture.Capacity`, `*branchFixture.ActiveHeader`, `*branchFixture.Commit`, `*branchFixture.Rewind`, `opts`, `index`, `seed`, `fork`, `TestIndexRangeRestartAndConfirmations`, `TestIndexFollowHeadAndCapacity`, `TestIndexShallowReorgAndRetainedHistory`, `TestIndexStopsWithoutProvenAncestor`, `TestIndexFailuresDoNotAdvance`, `TestIndexUint64BoundaryAndDepthUnderflow`, `*branchFixture.OpenReindex`, `*branchFixture.CommitReindex`, `*branchFixture.Replay`, `TestReindexRestartsWithoutChangingLiveCursor`, `TestIndexRejectsSameIdentityWithChangedParent`, `TestIndexProgressPreservesBatchAndFailedIdentity`, `TestFailedBatchRetainsEarlierCommittedIdentity`, `TestReindexAmbiguousCommitKeepsIdentity`, `*activeReadFailure.ActiveHeader`, `TestActiveReadFailureDoesNotInventMissingAncestry`, `*exhaustedObserver.Header`, `TestExecutionBudgetPreservesDurableProgress`.

- [reindex.go](../../services/extractor-service/internal/application/usecases/reindex.go).
  Types and interfaces: `Reindex`.
  Functions and methods: `NewReindex`, `*Reindex.Cycle`, `Replay`.

## `services/extractor-service/internal/config`

- [config.go](../../services/extractor-service/internal/config/config.go).
  Types and interfaces: `Config`.
  Functions and methods: `Load`.

- [config_test.go](../../services/extractor-service/internal/config/config_test.go) — tests.
  Functions and methods: `TestLoad`.

- [dispatch.go](../../services/extractor-service/internal/config/dispatch.go).
  Types and interfaces: `DispatchConfig`.
  Functions and methods: `loadDispatch`.

- [dispatch_test.go](../../services/extractor-service/internal/config/dispatch_test.go) — tests.
  Functions and methods: `TestDispatchConfiguration`.

- [execution.go](../../services/extractor-service/internal/config/execution.go).
  Functions and methods: `loadExecutionSafety`.

- [execution_test.go](../../services/extractor-service/internal/config/execution_test.go) — tests.
  Functions and methods: `TestMainnetExecutionSafety`, `TestBoundedMainnetReindexAndOfflineDrain`.

- [indexing.go](../../services/extractor-service/internal/config/indexing.go).
  Types and interfaces: `IndexingConfig`.
  Functions and methods: `loadIndexing`, `uintSetting`, `durationSetting`.

- [indexing_test.go](../../services/extractor-service/internal/config/indexing_test.go) — tests.
  Functions and methods: `indexEnvironment`, `TestIndexingConfiguration`, `TestOperatorModes`, `TestReceiptConcurrencyConfiguration`.

## `services/extractor-service/internal/domain`

- [block.go](../../services/extractor-service/internal/domain/block.go).
  Types and interfaces: `ChainID`, `Hash`, `Address`, `BlockID`, `Header`, `Transaction`, `Receipt`, `Log`, `Extraction`.
  Functions and methods: `ChainID.Validate`, `BlockID.Validate`, `BlockID.EventID`, `Extraction.Validate`, `sameBlock`, `Extraction.Clone`.
  Package values: `ErrInvalidData`, `ErrChainMismatch`, `ErrUnavailable`, `ErrIncomplete`, `ErrBlockConflict`, `ErrCheckpointConflict`, `ErrOutOfSequence`.

- [block_test.go](../../services/extractor-service/internal/domain/block_test.go) — tests.
  Functions and methods: `validExtraction`, `TestChainID`, `TestExtractionValidation`, `TestGenesis`, `TestCloneOwnershipAndEventIdentity`, `TestCheckpointPosition`, `TestCheckpointContinuity`.

- [checkpoint.go](../../services/extractor-service/internal/domain/checkpoint.go).
  Types and interfaces: `Checkpoint`.
  Functions and methods: `CheckPosition`, `CheckContinuity`.

- [indexing.go](../../services/extractor-service/internal/domain/indexing.go).
  Types and interfaces: `CapacityLimits`, `Capacity`.
  Functions and methods: `EligibleHeight`, `CapacityLimits.Validate`, `Capacity.Allows`, `ValidateOperationID`.
  Package values: `ErrRecoveryRequired`, `ErrUnstableChain`, `ErrBackpressure`.

- [indexing_test.go](../../services/extractor-service/internal/domain/indexing_test.go) — tests.
  Functions and methods: `TestEligibility`, `TestCapacityAndOperationIdentity`.

## `services/loader-service/cmd/loader`

- [main.go](../../services/loader-service/cmd/loader/main.go).
  Functions and methods: `main`, `run`.

## `services/loader-service/internal/adapters/inbound/grpc`

- [logging_test.go](../../services/loader-service/internal/adapters/inbound/grpc/logging_test.go) — tests.
  Functions and methods: `TestAcceptanceLogsDurableIdentityAndRedactsFailures`.

- [receiver.go](../../services/loader-service/internal/adapters/inbound/grpc/receiver.go).
  Types and interfaces: `Receiver`.
  Functions and methods: `NewReceiver`, `NewServer`, `NewServerWithAdmission`, `*Receiver.SubmitTransformedBlock`, `acceptanceError`, `transportErrorCode`.

- [receiver_test.go](../../services/loader-service/internal/adapters/inbound/grpc/receiver_test.go) — tests.
  Types and interfaces: `acceptFunc`.
  Functions and methods: `acceptFunc.AcceptTransformedBlock`, `request`, `receiver`, `TestMappingAndDurableOutcomes`, `TestValidationNeverDelegates`, `TestAcceptanceFailuresNeverACK`, `TestNoACKBeforePortCompletes`, `TestCanceledBeforeAcceptance`, `TestConstructorRejectsInvalidConfig`, `TestGRPCSkeletonAndLargeMessage`, `TestExactPayloadLimitAndEmptyObject`.

## `services/loader-service/internal/adapters/inbound/metrics`

- [handler.go](../../services/loader-service/internal/adapters/inbound/metrics/handler.go).
  Functions and methods: `Handler`.

## `services/loader-service/internal/adapters/inbound/worker`

- [logging_test.go](../../services/loader-service/internal/adapters/inbound/worker/logging_test.go) — tests.
  Functions and methods: `TestCorrelatedProgressSurvivesCancellationAndErrors`, `TestClaimFailureDoesNotInventIdentity`.

- [metrics_test.go](../../services/loader-service/internal/adapters/inbound/worker/metrics_test.go) — tests.
  Functions and methods: `TestMetricsIndependentOfLogLevelAndShutdown`.

- [worker.go](../../services/loader-service/internal/adapters/inbound/worker/worker.go).
  Types and interfaces: `Worker`.
  Functions and methods: `New`, `*Worker.Run`.

- [worker_test.go](../../services/loader-service/internal/adapters/inbound/worker/worker_test.go) — tests.
  Types and interfaces: `processorFunc`.
  Functions and methods: `processorFunc.ProcessOne`, `await`, `TestBoundedConcurrencyAndJoinedShutdown`, `TestDeadlineIdleAndErrorPolling`, `TestPendingWorkDrainsWithoutPollDelay`.

## `services/loader-service/internal/adapters/logging`

- [errors.go](../../services/loader-service/internal/adapters/logging/errors.go).
  Functions and methods: `ErrorCode`.

## `services/loader-service/internal/adapters/outbound/jsondocument`

- [decode.go](../../services/loader-service/internal/adapters/outbound/jsondocument/decode.go).
  Types and interfaces: `Decoder`.
  Functions and methods: `Decoder.Decode`, `textValue`, `timestamp`, `blockFrom`, `transactionFrom`, `receiptFrom`, `logFrom`, `accessFrom`, `authorizationFrom`, `accesses`, `authorizations`.

- [decode_test.go](../../services/loader-service/internal/adapters/outbound/jsondocument/decode_test.go) — tests.
  Functions and methods: `fixture`, `TestTransformerV1Compatibility`, `TestRejectedDocuments`, `TestEmptyAndHistoricalDocuments`, `TestBlobAndAuthorizationFields`.

- [fields.go](../../services/loader-service/internal/adapters/outbound/jsondocument/fields.go).
  Types and interfaces: `object`, `reader`.
  Functions and methods: `read`, `optional`, `array`, `plainObject`, `objectOf`, `walk`, `number`, `quantity`, `chainID`, `uintValue`, `data`, `hash`, `address`, `bloom`, `nonce`, `hashes`, `boolean`.

## `services/loader-service/internal/adapters/outbound/metrics`

- [queue.go](../../services/loader-service/internal/adapters/outbound/metrics/queue.go).
  Types and interfaces: `Queue`.
  Functions and methods: `Queue.Complete`.

- [queue_test.go](../../services/loader-service/internal/adapters/outbound/metrics/queue_test.go) — tests.
  Types and interfaces: `queueFake`.
  Functions and methods: `*queueFake.Complete`, `TestPersistenceOutcomeDoesNotChangeCommitSemantics`.

## `services/loader-service/internal/adapters/outbound/postgres`

- [admission.go](../../services/loader-service/internal/adapters/outbound/postgres/admission.go).
  Functions and methods: `*Store.admissionFull`.

- [admission_integration_test.go](../../services/loader-service/internal/adapters/outbound/postgres/admission_integration_test.go) — tests.
  Functions and methods: `testAdmission`.

- [batch.go](../../services/loader-service/internal/adapters/outbound/postgres/batch.go).
  Types and interfaces: `recordExecutor`, `batchExecutor`, `recordBatch`.
  Functions and methods: `*recordBatch.Exec`, `*recordBatch.flush`, `statementSize`, `argumentSize`.
  Package values: `DefaultBatchSize`, `MaxBatchSize`, `batchByteBudget`, `maxStatementParameters`.

- [batch_integration_test.go](../../services/loader-service/internal/adapters/outbound/postgres/batch_integration_test.go) — tests.
  Functions and methods: `testBatches`.

- [batch_test.go](../../services/loader-service/internal/adapters/outbound/postgres/batch_test.go) — tests.
  Types and interfaces: `recordedStatement`, `recordingExecutor`, `recordingResults`, `batchFailureDB`, `batchFailureTx`, `claimedRow`.
  Functions and methods: `*recordingExecutor.Exec`, `*recordingExecutor.SendBatch`, `*recordingResults.Exec`, `*recordingResults.Close`, `largeBatchPayload`, `writeTestSnapshot`, `TestRecordBatchPreservesAllSQLValuesAndOrder`, `TestRecordBatchByteAndParameterBounds`, `TestRecordBatchErrorsAndCancellation`, `batchFailureDB.BeginTx`, `*batchFailureTx.Exec`, `*batchFailureTx.SendBatch`, `*batchFailureTx.QueryRow`, `*batchFailureTx.Rollback`, `*batchFailureTx.Commit`, `claimedRow.Scan`, `TestCompleteRollsBackAfterEarlierSuccessfulBatch`, `TestRecordBatchTransactionsWithoutLogs`.

- [complete.go](../../services/loader-service/internal/adapters/outbound/postgres/complete.go).
  Functions and methods: `*Store.Complete`.

- [integration_test.go](../../services/loader-service/internal/adapters/outbound/postgres/integration_test.go) — tests.
  Types and interfaces: `faultDB`, `faultTx`.
  Functions and methods: `integrationPool`, `testEvent`, `wire`, `emptyEvent`, `faultDB.BeginTx`, `faultTx.Commit`, `TestPostgresIntegration`.

- [metrics_integration_test.go](../../services/loader-service/internal/adapters/outbound/postgres/metrics_integration_test.go) — tests.
  Functions and methods: `testMetrics`.

- [observations.go](../../services/loader-service/internal/adapters/outbound/postgres/observations.go).
  Functions and methods: `updateObservations`.

- [observations_integration_test.go](../../services/loader-service/internal/adapters/outbound/postgres/observations_integration_test.go) — tests.
  Functions and methods: `forkEvent`, `testObservations`.

- [readiness.go](../../services/loader-service/internal/adapters/outbound/postgres/readiness.go).
  Functions and methods: `*Store.CheckReadiness`.

- [readiness_integration_test.go](../../services/loader-service/internal/adapters/outbound/postgres/readiness_integration_test.go) — tests.
  Functions and methods: `testReadiness`.

- [readiness_policy.go](../../services/loader-service/internal/adapters/outbound/postgres/readiness_policy.go).
  Package values: `readinessPolicy`.

- [readiness_policy_test.go](../../services/loader-service/internal/adapters/outbound/postgres/readiness_policy_test.go) — tests.
  Functions and methods: `TestReadinessMigrationManifest`.

- [records.go](../../services/loader-service/internal/adapters/outbound/postgres/records.go).
  Functions and methods: `hashArray`, `optionalValue`, `hexData`, `accessJSON`, `authorizationJSON`, `insertBlock`, `insertTransaction`, `insertReceipt`, `insertLog`, `dbErrorOrNil`.

- [repair.go](../../services/loader-service/internal/adapters/outbound/postgres/repair.go).
  Functions and methods: `*Store.Repair`.

- [repair_integration_test.go](../../services/loader-service/internal/adapters/outbound/postgres/repair_integration_test.go) — tests.
  Functions and methods: `testRepair`.

- [status.go](../../services/loader-service/internal/adapters/outbound/postgres/status.go).
  Functions and methods: `*Store.ReadStatus`.

- [store.go](../../services/loader-service/internal/adapters/outbound/postgres/store.go).
  Types and interfaces: `database`, `Store`.
  Functions and methods: `New`, `NewWithBatchSize`, `dbError`, `rollback`, `commit`, `*Store.begin`, `*Store.Accept`, `scanItem`, `*Store.Claim`, `*Store.Fail`.
  Package values: `eventColumns`.

## `services/loader-service/internal/application/ports/inbound`

- [accept_block.go](../../services/loader-service/internal/application/ports/inbound/accept_block.go).
  Types and interfaces: `BlockIdentity`, `AcceptTransformedBlockRequest`, `Acceptance`, `AcceptanceResult`, `TransformedBlockAcceptor`.
  Package values: `ErrInvalidEvent`, `ErrUnsupportedVersion`, `ErrConflict`, `ErrOverloaded`, `ErrUnavailable`, `ErrNotImplemented`, `AcceptanceUnspecified`, `DurablyAccepted`, `AlreadyAccepted`.

- [process.go](../../services/loader-service/internal/application/ports/inbound/process.go).
  Types and interfaces: `ProcessResult`, `Processor`.

- [validation.go](../../services/loader-service/internal/application/ports/inbound/validation.go).
  Functions and methods: `ValidateEnvelope`, `ValidateDocument`.
  Package values: `providerPattern`.

## `services/loader-service/internal/application/ports/outbound`

- [processing.go](../../services/loader-service/internal/application/ports/outbound/processing.go).
  Types and interfaces: `Item`, `Failure`, `Inbox`, `Queue`, `Decoder`.
  Package values: `ErrLeaseLost`, `ErrCommitUnknown`.

- [readiness.go](../../services/loader-service/internal/application/ports/outbound/readiness.go).
  Types and interfaces: `ReadinessChecker`.

- [repair.go](../../services/loader-service/internal/application/ports/outbound/repair.go).
  Types and interfaces: `RepairRequest`, `RepairStore`.
  Functions and methods: `RepairRequest.Validate`.
  Package values: `ErrRepairRefused`.

- [status.go](../../services/loader-service/internal/application/ports/outbound/status.go).
  Types and interfaces: `QueueStatus`, `Status`, `StatusReader`.

## `services/loader-service/internal/application/usecases`

- [accept.go](../../services/loader-service/internal/application/usecases/accept.go).
  Types and interfaces: `Accept`.
  Functions and methods: `NewAccept`, `*Accept.AcceptTransformedBlock`.

- [process.go](../../services/loader-service/internal/application/usecases/process.go).
  Types and interfaces: `Process`.
  Functions and methods: `NewProcess`, `*Process.ProcessOne`, `*Process.decode`, `classify`, `*Process.backoff`.

- [repair.go](../../services/loader-service/internal/application/usecases/repair.go).
  Functions and methods: `Repair`.

- [repair_test.go](../../services/loader-service/internal/application/usecases/repair_test.go) — tests.
  Types and interfaces: `repairFake`.
  Functions and methods: `*repairFake.Repair`, `TestRepairValidation`.

- [usecases_test.go](../../services/loader-service/internal/application/usecases/usecases_test.go) — tests.
  Types and interfaces: `fakeStore`.
  Functions and methods: `*fakeStore.Accept`, `*fakeStore.Claim`, `*fakeStore.Complete`, `*fakeStore.Fail`, `event`, `processor`, `TestAcceptanceDurableOutcomes`, `TestEnvelopeRejectedBeforeIO`, `TestProcessingAndProvenanceBinding`, `TestRetryCancellationAndAmbiguousCommit`.

## `services/loader-service/internal/config`

- [admission_test.go](../../services/loader-service/internal/config/admission_test.go) — tests.
  Functions and methods: `TestAdmissionLimits`.

- [batch_test.go](../../services/loader-service/internal/config/batch_test.go) — tests.
  Functions and methods: `TestSQLBatchSize`.

- [config.go](../../services/loader-service/internal/config/config.go).
  Types and interfaces: `Config`.
  Functions and methods: `Load`.

- [config_test.go](../../services/loader-service/internal/config/config_test.go) — tests.
  Functions and methods: `TestLoad`, `setDefaults`, `TestProcessingConfiguration`.

- [repair.go](../../services/loader-service/internal/config/repair.go).
  Functions and methods: `loadRepair`.

- [repair_test.go](../../services/loader-service/internal/config/repair_test.go) — tests.
  Functions and methods: `TestRepairConfiguration`.

## `services/loader-service/internal/domain`

- [document.go](../../services/loader-service/internal/domain/document.go).
  Types and interfaces: `Document`.

- [model.go](../../services/loader-service/internal/domain/model.go).
  Types and interfaces: `Quantity`, `ChainID`, `Hash`, `Address`, `Bloom`, `Nonce`, `Optional`, `Snapshot`, `Block`, `Transaction`, `Receipt`, `Log`, `Access`, `Authorization`.
  Functions and methods: `Optional[T].HasValue`.

- [observation.go](../../services/loader-service/internal/domain/observation.go).
  Types and interfaces: `BlockReference`, `CanonicalStatus`, `ParentLink`, `Observation`.
  Functions and methods: `ObservationRange`, `ObserveArrival`.
  Package values: `CanonicalUnknown`, `ParentUnknown`, `ParentLinked`, `ParentMismatch`.

- [observation_test.go](../../services/loader-service/internal/domain/observation_test.go) — tests.
  Functions and methods: `TestObserveArrival`, `TestObservationBoundaries`.

- [provenance.go](../../services/loader-service/internal/domain/provenance.go).
  Types and interfaces: `Provenance`.

- [validation.go](../../services/loader-service/internal/domain/validation.go).
  Types and interfaces: `ValidationError`.
  Functions and methods: `*ValidationError.Error`, `*ValidationError.Unwrap`, `Invalid`, `Decimal`, `Quantity.Valid`, `ChainID.Valid`, `Required`, `Kind`, `sameRecipient`, `cmp`, `Snapshot.Validate`.
  Package values: `ErrInvalid`, `ErrIncomplete`, `ErrIdentity`, `ErrUnsupported`.

- [validation_test.go](../../services/loader-service/internal/domain/validation_test.go) — tests.
  Functions and methods: `validSnapshot`, `TestSnapshotValidation`, `TestExactQuantities`, `TestLogsAndDuplicateTransactions`.

- [values.go](../../services/loader-service/internal/domain/values.go).
  Functions and methods: `Snapshot.validateValues`, `Block.validateValues`, `Transaction.validateValues`, `Receipt.validateValues`, `Log.validateValues`, `Access.validateValues`, `Authorization.validateValues`.

## `services/transformer-service/cmd/transformer`

- [main.go](../../services/transformer-service/cmd/transformer/main.go).
  Functions and methods: `main`, `run`.

## `services/transformer-service/internal/adapters/inbound/dispatcher`

- [logging_test.go](../../services/transformer-service/internal/adapters/inbound/dispatcher/logging_test.go) — tests.
  Functions and methods: `TestCorrelatedProgressSurvivesCancellationAndErrors`, `TestClaimFailureDoesNotInventIdentity`.

- [metrics_test.go](../../services/transformer-service/internal/adapters/inbound/dispatcher/metrics_test.go) — tests.
  Functions and methods: `TestMetricsIndependentOfLogLevelAndShutdown`.

- [worker.go](../../services/transformer-service/internal/adapters/inbound/dispatcher/worker.go).
  Types and interfaces: `Worker`.
  Functions and methods: `New`, `*Worker.Run`.

- [worker_test.go](../../services/transformer-service/internal/adapters/inbound/dispatcher/worker_test.go) — tests.
  Types and interfaces: `processorFunc`.
  Functions and methods: `processorFunc.DispatchOne`, `await`, `TestBoundedConcurrencyAndJoinedShutdown`, `TestDeadlineIdleAndErrorPolling`, `TestPendingWorkDrainsWithoutPollDelay`.

## `services/transformer-service/internal/adapters/inbound/grpc`

- [logging_test.go](../../services/transformer-service/internal/adapters/inbound/grpc/logging_test.go) — tests.
  Functions and methods: `TestAcceptanceLogsDurableIdentityAndRedactsFailures`.

- [receiver.go](../../services/transformer-service/internal/adapters/inbound/grpc/receiver.go).
  Types and interfaces: `Receiver`.
  Functions and methods: `NewReceiver`, `NewServer`, `NewServerWithAdmission`, `*Receiver.SubmitRawBlock`, `acceptanceError`, `transportErrorCode`.

- [receiver_test.go](../../services/transformer-service/internal/adapters/inbound/grpc/receiver_test.go) — tests.
  Types and interfaces: `acceptFunc`.
  Functions and methods: `acceptFunc.AcceptRawBlock`, `request`, `receiver`, `TestMappingAndDurableOutcomes`, `TestValidationNeverDelegates`, `TestAcceptanceFailuresNeverACK`, `TestNoACKBeforePortCompletes`, `TestCanceledBeforeAcceptance`, `TestConstructorRejectsInvalidConfig`, `TestGRPCSkeletonAndLargeMessage`, `TestExactPayloadLimitAndEmptyObject`.

## `services/transformer-service/internal/adapters/inbound/metrics`

- [handler.go](../../services/transformer-service/internal/adapters/inbound/metrics/handler.go).
  Functions and methods: `Handler`.

## `services/transformer-service/internal/adapters/inbound/worker`

- [logging_test.go](../../services/transformer-service/internal/adapters/inbound/worker/logging_test.go) — tests.
  Functions and methods: `TestCorrelatedProgressSurvivesCancellationAndErrors`, `TestClaimFailureDoesNotInventIdentity`.

- [metrics_test.go](../../services/transformer-service/internal/adapters/inbound/worker/metrics_test.go) — tests.
  Functions and methods: `TestMetricsIndependentOfLogLevelAndShutdown`.

- [worker.go](../../services/transformer-service/internal/adapters/inbound/worker/worker.go).
  Types and interfaces: `Worker`.
  Functions and methods: `New`, `*Worker.Run`.

- [worker_test.go](../../services/transformer-service/internal/adapters/inbound/worker/worker_test.go) — tests.
  Types and interfaces: `processorFunc`.
  Functions and methods: `processorFunc.ProcessOne`, `await`, `TestBoundedConcurrencyAndJoinedShutdown`, `TestDeadlineIdleAndErrorPolling`, `TestPendingWorkDrainsWithoutPollDelay`.

## `services/transformer-service/internal/adapters/logging`

- [errors.go](../../services/transformer-service/internal/adapters/logging/errors.go).
  Functions and methods: `ErrorCode`.

## `services/transformer-service/internal/adapters/outbound/ethereumjson`

- [decode.go](../../services/transformer-service/internal/adapters/outbound/ethereumjson/decode.go).
  Types and interfaces: `Decoder`.
  Functions and methods: `Decoder.Decode`, `blockFrom`, `transactionFrom`, `receiptFrom`, `logFrom`, `accessFrom`, `authorizationFrom`, `accesses`, `authorizations`.

- [decode_test.go](../../services/transformer-service/internal/adapters/outbound/ethereumjson/decode_test.go) — tests.
  Functions and methods: `fixtures`, `TestDeterministicExactOutput`, `TestMalformedAndIncompleteInput`, `TestHistoricalAndOptionalValues`, `TestTypedTransactionNormalization`, `TestEmptyBlockCollections`.

- [fields.go](../../services/transformer-service/internal/adapters/outbound/ethereumjson/fields.go).
  Types and interfaces: `object`, `reader`.
  Functions and methods: `read`, `optional`, `array`, `plainObject`, `objectOf`, `walk`, `number`, `quantity`, `chainID`, `uintValue`, `data`, `hash`, `address`, `bloom`, `nonce`, `hashes`, `boolean`.

## `services/transformer-service/internal/adapters/outbound/grpc`

- [publisher.go](../../services/transformer-service/internal/adapters/outbound/grpc/publisher.go).
  Types and interfaces: `Publisher`.
  Functions and methods: `New`, `*Publisher.Close`, `*Publisher.Publish`.

- [publisher_test.go](../../services/transformer-service/internal/adapters/outbound/grpc/publisher_test.go) — tests.
  Types and interfaces: `recipient`, `publishClientFunc`.
  Functions and methods: `*recipient.SubmitTransformedBlock`, `publication`, `testPublisher`, `TestPublishVerifiedACKAndExactRetry`, `TestPublishFailures`, `publishClientFunc.SubmitTransformedBlock`, `TestPublishLocalBudgetsAndCancellation`, `TestPublishCancellationInFlight`, `TestPublishReceiverOverloadIsRetryable`.

## `services/transformer-service/internal/adapters/outbound/jsondocument`

- [encode.go](../../services/transformer-service/internal/adapters/outbound/jsondocument/encode.go).
  Types and interfaces: `Encoder`.
  Functions and methods: `New`, `*Encoder.Encode`, `hexData`, `hashes`, `accesses`, `authorizations`, `put`, `blockObject`, `transactionObject`, `receiptObject`, `logObject`, `accessObject`, `authorizationObject`.

## `services/transformer-service/internal/adapters/outbound/postgres`

- [admission.go](../../services/transformer-service/internal/adapters/outbound/postgres/admission.go).
  Functions and methods: `*Store.admissionFull`.

- [admission_integration_test.go](../../services/transformer-service/internal/adapters/outbound/postgres/admission_integration_test.go) — tests.
  Functions and methods: `testAdmission`.

- [integration_test.go](../../services/transformer-service/internal/adapters/outbound/postgres/integration_test.go) — tests.
  Types and interfaces: `faultDB`, `faultTx`.
  Functions and methods: `integrationPool`, `testEvent`, `wire`, `faultDB.BeginTx`, `faultTx.Commit`, `TestPostgresIntegration`.

- [metrics_integration_test.go](../../services/transformer-service/internal/adapters/outbound/postgres/metrics_integration_test.go) — tests.
  Functions and methods: `testMetrics`.

- [outbox.go](../../services/transformer-service/internal/adapters/outbound/postgres/outbox.go).
  Types and interfaces: `Outbox`.
  Functions and methods: `NewOutbox`, `deliveryError`, `deliveryRollback`, `deliveryCommit`, `*Outbox.begin`, `*Outbox.ClaimDelivery`, `*Outbox.Published`, `*Outbox.RetryDelivery`.

- [outbox_integration_test.go](../../services/transformer-service/internal/adapters/outbound/postgres/outbox_integration_test.go) — tests.
  Types and interfaces: `deliveryFaultDB`, `deliveryFaultTx`.
  Functions and methods: `deliveryFaultDB.BeginTx`, `deliveryFaultTx.Commit`, `testOutbox`.

- [readiness.go](../../services/transformer-service/internal/adapters/outbound/postgres/readiness.go).
  Functions and methods: `*Store.CheckReadiness`.

- [readiness_integration_test.go](../../services/transformer-service/internal/adapters/outbound/postgres/readiness_integration_test.go) — tests.
  Functions and methods: `testReadiness`.

- [readiness_policy.go](../../services/transformer-service/internal/adapters/outbound/postgres/readiness_policy.go).
  Package values: `readinessPolicy`.

- [readiness_policy_test.go](../../services/transformer-service/internal/adapters/outbound/postgres/readiness_policy_test.go) — tests.
  Functions and methods: `TestReadinessMigrationManifest`.

- [repair.go](../../services/transformer-service/internal/adapters/outbound/postgres/repair.go).
  Functions and methods: `*Store.Repair`.

- [repair_integration_test.go](../../services/transformer-service/internal/adapters/outbound/postgres/repair_integration_test.go) — tests.
  Functions and methods: `testRepair`.

- [status.go](../../services/transformer-service/internal/adapters/outbound/postgres/status.go).
  Functions and methods: `*Store.ReadStatus`.

- [store.go](../../services/transformer-service/internal/adapters/outbound/postgres/store.go).
  Types and interfaces: `database`, `Store`.
  Functions and methods: `New`, `dbError`, `rollback`, `commit`, `*Store.begin`, `*Store.Accept`, `scanItem`, `*Store.Claim`, `*Store.Complete`, `*Store.Fail`.
  Package values: `eventColumns`.

## `services/transformer-service/internal/application/ports/inbound`

- [accept_block.go](../../services/transformer-service/internal/application/ports/inbound/accept_block.go).
  Types and interfaces: `BlockIdentity`, `AcceptRawBlockRequest`, `Acceptance`, `AcceptanceResult`, `RawBlockAcceptor`.
  Package values: `ErrInvalidEvent`, `ErrUnsupportedVersion`, `ErrConflict`, `ErrOverloaded`, `ErrUnavailable`, `ErrNotImplemented`, `AcceptanceUnspecified`, `DurablyAccepted`, `AlreadyAccepted`.

- [dispatch.go](../../services/transformer-service/internal/application/ports/inbound/dispatch.go).
  Types and interfaces: `DispatchResult`, `Dispatcher`.

- [process.go](../../services/transformer-service/internal/application/ports/inbound/process.go).
  Types and interfaces: `ProcessResult`, `Processor`.

- [validation.go](../../services/transformer-service/internal/application/ports/inbound/validation.go).
  Functions and methods: `ValidateEnvelope`.
  Package values: `providerPattern`.

## `services/transformer-service/internal/application/ports/outbound`

- [delivery.go](../../services/transformer-service/internal/application/ports/outbound/delivery.go).
  Types and interfaces: `Publication`, `Delivery`, `DeliveryFailure`, `DeliveryQueue`, `Publisher`.
  Package values: `ErrDeliveryLeaseLost`, `ErrDeliveryCommitUnknown`, `ErrPublishInvalid`, `ErrPublishConflict`, `ErrPublishOversize`, `ErrPublishOverloaded`, `ErrPublishUnavailable`, `ErrPublishACK`.

- [processing.go](../../services/transformer-service/internal/application/ports/outbound/processing.go).
  Types and interfaces: `Item`, `Result`, `Failure`, `Inbox`, `Queue`, `Decoder`, `Encoder`.
  Package values: `ErrLeaseLost`, `ErrCommitUnknown`, `ErrOutputTooLarge`.

- [readiness.go](../../services/transformer-service/internal/application/ports/outbound/readiness.go).
  Types and interfaces: `ReadinessChecker`.

- [repair.go](../../services/transformer-service/internal/application/ports/outbound/repair.go).
  Types and interfaces: `RepairRequest`, `RepairStore`.
  Functions and methods: `RepairRequest.Validate`.
  Package values: `ErrRepairRefused`.

- [status.go](../../services/transformer-service/internal/application/ports/outbound/status.go).
  Types and interfaces: `QueueStatus`, `Status`, `StatusReader`.

## `services/transformer-service/internal/application/usecases`

- [accept.go](../../services/transformer-service/internal/application/usecases/accept.go).
  Types and interfaces: `Accept`.
  Functions and methods: `NewAccept`, `*Accept.AcceptRawBlock`.

- [dispatch.go](../../services/transformer-service/internal/application/usecases/dispatch.go).
  Types and interfaces: `Dispatch`.
  Functions and methods: `NewDispatch`, `*Dispatch.DispatchOne`, `*Dispatch.deliveryBackoff`.

- [dispatch_test.go](../../services/transformer-service/internal/application/usecases/dispatch_test.go) — tests.
  Types and interfaces: `deliveryFake`, `publisherFunc`.
  Functions and methods: `*deliveryFake.ClaimDelivery`, `*deliveryFake.Published`, `*deliveryFake.RetryDelivery`, `publisherFunc.Publish`, `TestDispatchAcknowledgementBoundary`, `TestDispatchTimeoutCancellationExhaustionAndIdle`, `TestOverloadExhaustsDurableAllowance`.

- [process.go](../../services/transformer-service/internal/application/usecases/process.go).
  Types and interfaces: `Process`.
  Functions and methods: `NewProcess`, `*Process.ProcessOne`, `*Process.transform`, `classify`, `*Process.backoff`.

- [repair.go](../../services/transformer-service/internal/application/usecases/repair.go).
  Functions and methods: `Repair`.

- [repair_test.go](../../services/transformer-service/internal/application/usecases/repair_test.go) — tests.
  Types and interfaces: `repairFake`.
  Functions and methods: `*repairFake.Repair`, `TestRepairValidation`.

- [usecases_test.go](../../services/transformer-service/internal/application/usecases/usecases_test.go) — tests.
  Types and interfaces: `fakeStore`.
  Functions and methods: `*fakeStore.Accept`, `*fakeStore.Claim`, `*fakeStore.Complete`, `*fakeStore.Fail`, `event`, `processor`, `TestAcceptanceCommitAndFailures`, `TestApplicationEnvelopeValidation`, `TestProcessSuccessAndPoison`, `TestProcessingRetryAndAmbiguousCommit`, `TestBackoffAndOutputFailure`.

## `services/transformer-service/internal/config`

- [admission_test.go](../../services/transformer-service/internal/config/admission_test.go) — tests.
  Functions and methods: `TestAdmissionLimits`.

- [config.go](../../services/transformer-service/internal/config/config.go).
  Types and interfaces: `Config`.
  Functions and methods: `Load`.

- [config_test.go](../../services/transformer-service/internal/config/config_test.go) — tests.
  Functions and methods: `TestLoad`, `setDefaults`, `TestProcessingConfiguration`.

- [dispatch.go](../../services/transformer-service/internal/config/dispatch.go).
  Types and interfaces: `DispatchConfig`.
  Functions and methods: `loadDispatch`.

- [dispatch_test.go](../../services/transformer-service/internal/config/dispatch_test.go) — tests.
  Functions and methods: `TestDispatchConfiguration`.

- [repair.go](../../services/transformer-service/internal/config/repair.go).
  Functions and methods: `loadRepair`.

- [repair_test.go](../../services/transformer-service/internal/config/repair_test.go) — tests.
  Functions and methods: `TestRepairConfiguration`.

## `services/transformer-service/internal/domain`

- [model.go](../../services/transformer-service/internal/domain/model.go).
  Types and interfaces: `Quantity`, `ChainID`, `Hash`, `Address`, `Bloom`, `Nonce`, `Optional`, `Snapshot`, `Block`, `Transaction`, `Receipt`, `Log`, `Access`, `Authorization`.
  Functions and methods: `Optional[T].HasValue`.

- [provenance.go](../../services/transformer-service/internal/domain/provenance.go).
  Types and interfaces: `Provenance`.

- [validation.go](../../services/transformer-service/internal/domain/validation.go).
  Types and interfaces: `ValidationError`.
  Functions and methods: `*ValidationError.Error`, `*ValidationError.Unwrap`, `Invalid`, `Decimal`, `Quantity.Valid`, `ChainID.Valid`, `Required`, `Kind`, `sameRecipient`, `cmp`, `Snapshot.Validate`.
  Package values: `ErrInvalid`, `ErrIncomplete`, `ErrIdentity`, `ErrUnsupported`.

- [validation_test.go](../../services/transformer-service/internal/domain/validation_test.go) — tests.
  Functions and methods: `validSnapshot`, `TestSnapshotValidation`, `TestExactQuantities`, `TestLogsAndDuplicateTransactions`.

- [values.go](../../services/transformer-service/internal/domain/values.go).
  Functions and methods: `Snapshot.validateValues`, `Block.validateValues`, `Transaction.validateValues`, `Receipt.validateValues`, `Log.validateValues`, `Access.validateValues`, `Authorization.validateValues`.

## `test/architecture`

- [boundaries_test.go](../../test/architecture/boundaries_test.go) — tests.
  Functions and methods: `within`, `importViolation`, `TestHexagonalArchitecture`, `foreignSchemas`, `goSchemaViolations`, `constantString`.
  Package values: `module`, `pureImports`, `schemaReference`, `sqlComments`.

- [policy_test.go](../../test/architecture/policy_test.go) — tests.
  Functions and methods: `TestImportPolicyRejectsBoundaryViolations`, `TestSchemaPolicy`, `TestGoSchemaInspection`.

## `test/ci`

- [integration_test.go](../../test/ci/integration_test.go) — tests.
  Functions and methods: `TestIntegrationGate`.

## `test/cmd/ethereum-fixture`

- [main.go](../../test/cmd/ethereum-fixture/main.go).
  Functions and methods: `main`, `run`.

## `test/fixture`

- [chain.go](../../test/fixture/chain.go).
  Functions and methods: `Hash`, `TxHash`, `digest`, `quantity`, `Block`.
  Package values: `Start`, `Last`.

- [proxy.go](../../test/fixture/proxy.go).
  Types and interfaces: `RawProxy`, `TransformedProxy`.
  Functions and methods: `*RawProxy.SubmitRawBlock`, `*TransformedProxy.SubmitTransformedBlock`, `*Server.accepted`.

- [server.go](../../test/fixture/server.go).
  Types and interfaces: `State`, `Server`.
  Functions and methods: `New`, `*Server.Snapshot`, `*Server.Handler`, `*Server.control`, `*Server.rpc`.

- [server_test.go](../../test/fixture/server_test.go) — tests.
  Types and interfaces: `rawClient`.
  Functions and methods: `request`, `TestDeterministicChainAndReorg`, `TestInvalidControlAndUnknownRPC`, `*rawClient.SubmitRawBlock`, `TestLostACKRequiresRealAcceptance`.

## `test/integration/cmd/extractor-postgres`

- [main.go](../../test/integration/cmd/extractor-postgres/main.go).
  Types and interfaces: `command`, `outputBuffer`.
  Functions and methods: `main`, `start`, `randomHex`, `isolatedEnvironment`, `run`, `*outputBuffer.Write`, `*outputBuffer.safe`, `execute`, `retainResources`.
  Package values: `composeFile`, `passwordNames`.

- [main_test.go](../../test/integration/cmd/extractor-postgres/main_test.go) — tests.
  Functions and methods: `argAfter`, `TestLifecycle`, `TestCancellationAndCleanupError`, `TestUniqueProjects`, `TestOutputRedactionAndBounds`, `TestPreserveToolSettings`, `TestRetainResources`.

## `test/integration/cmd/fixture-e2e`

- [main.go](../../test/integration/cmd/fixture-e2e/main.go).
  Types and interfaces: `command`, `outputBuffer`.
  Functions and methods: `main`, `start`, `randomHex`, `isolatedEnvironment`, `run`, `*outputBuffer.Write`, `*outputBuffer.safe`, `execute`, `initialize`, `retainResources`.
  Package values: `composeFile`, `passwordNames`.

- [main_test.go](../../test/integration/cmd/fixture-e2e/main_test.go) — tests.
  Functions and methods: `argAfter`, `TestLifecycle`, `TestCancellationAndCleanupError`, `TestUniqueProjects`, `TestOutputRedactionAndBounds`, `TestPreserveToolSettings`, `TestInitializeDoesNotOverwrite`, `TestRetainResources`.

## `test/integration/cmd/handoff-postgres`

- [baseline.go](../../test/integration/cmd/handoff-postgres/baseline.go).
  Types and interfaces: `baselineRun`, `baselineOptions`.
  Functions and methods: `baselineFlags`, `baselineWorkers`, `startBaseline`.

- [baseline_test.go](../../test/integration/cmd/handoff-postgres/baseline_test.go) — tests.
  Functions and methods: `TestBaselineGuards`, `TestBaselineRunnerNoRaceOrRegression`, `TestBaselineLoaderBatchSize`.

- [main.go](../../test/integration/cmd/handoff-postgres/main.go).
  Types and interfaces: `command`, `outputBuffer`.
  Functions and methods: `main`, `start`, `randomHex`, `isolatedEnvironment`, `run`, `runCase`, `*outputBuffer.Write`, `*outputBuffer.safe`, `execute`, `retainResources`.
  Package values: `composeFile`, `passwordNames`.

- [main_test.go](../../test/integration/cmd/handoff-postgres/main_test.go) — tests.
  Functions and methods: `argAfter`, `TestLifecycle`, `TestCancellationAndCleanupError`, `TestUniqueProjects`, `TestOutputRedactionAndBounds`, `TestPreserveToolSettings`, `TestRetainResources`, `TestRealFixtureIsolatedRetainedLifecycle`, `TestMissingRealFixturesFailBeforeDocker`.

- [mainnet.go](../../test/integration/cmd/handoff-postgres/mainnet.go).
  Types and interfaces: `realCase`.
  Functions and methods: `startReal`.

## `test/integration/cmd/loader-postgres`

- [main.go](../../test/integration/cmd/loader-postgres/main.go).
  Types and interfaces: `command`, `outputBuffer`.
  Functions and methods: `main`, `start`, `randomHex`, `isolatedEnvironment`, `run`, `*outputBuffer.Write`, `*outputBuffer.safe`, `execute`, `retainResources`.
  Package values: `composeFile`, `passwordNames`.

- [main_test.go](../../test/integration/cmd/loader-postgres/main_test.go) — tests.
  Functions and methods: `argAfter`, `TestLifecycle`, `TestCancellationAndCleanupError`, `TestUniqueProjects`, `TestOutputRedactionAndBounds`, `TestPreserveToolSettings`, `TestRetainResources`.

## `test/integration/cmd/transformer-postgres`

- [main.go](../../test/integration/cmd/transformer-postgres/main.go).
  Types and interfaces: `command`, `outputBuffer`.
  Functions and methods: `main`, `start`, `randomHex`, `isolatedEnvironment`, `run`, `*outputBuffer.Write`, `*outputBuffer.safe`, `execute`, `retainResources`.
  Package values: `composeFile`, `passwordNames`.

- [main_test.go](../../test/integration/cmd/transformer-postgres/main_test.go) — tests.
  Functions and methods: `argAfter`, `TestLifecycle`, `TestCancellationAndCleanupError`, `TestUniqueProjects`, `TestOutputRedactionAndBounds`, `TestPreserveToolSettings`, `TestRetainResources`.

## `test/integration/fixture`

- [e2e_test.go](../../test/integration/fixture/e2e_test.go) — tests.
  Types and interfaces: `system`.
  Functions and methods: `*system.command`, `*system.compose`, `*system.until`, `*system.is`, `*system.require`, `*system.control`, `*system.state`, `*system.drained`, `*system.probe`, `*system.stop`, `pool`, `TestFixtureE2E`, `*system.history`, `*system.verifyRecords`, `*system.verifyCorrelation`, `*system.metrics`.

- [storage_test.go](../../test/integration/fixture/storage_test.go) — tests.
  Functions and methods: `*system.pg`, `*system.sql`, `*system.admin`, `*system.verifyStorageRelease`, `*system.restoreEmpty`, `writeEvidence`, `*system.storageSnapshot`, `*system.queryLiteral`, `*system.measureStorage`, `*system.explain`, `*system.restoreArchive`.

## `test/integration/handoff`

- [completion_test.go](../../test/integration/handoff/completion_test.go) — tests.
  Functions and methods: `completionQuery`, `completionTarget`, `verifyPartialCompletion`, `verifyCompleteAndFaults`.

- [handoff_test.go](../../test/integration/handoff/handoff_test.go) — tests.
  Types and interfaces: `child`, `rawProxy`, `transformedProxy`.
  Functions and methods: `launch`, `*child.stop`, `await`, `reserve`, `pool`, `queryInt`, `connection`, `*rawProxy.SubmitRawBlock`, `*transformedProxy.SubmitTransformedBlock`, `serve`, `TestHandoff`.

- [mainnet_test.go](../../test/integration/handoff/mainnet_test.go) — tests.
  Types and interfaces: `realInput`.
  Functions and methods: `readRealInput`, `assertRealPipeline`.

- [oracle_test.go](../../test/integration/handoff/oracle_test.go) — tests.
  Types and interfaces: `rpcField`.
  Functions and methods: `fields`, `recordFields`, `decodeObject`, `sqlValue`, `compareRecord`, `TestOfflineOracle`.

- [performance_integrity_test.go](../../test/integration/handoff/performance_integrity_test.go) — tests.
  Functions and methods: `assertBaselineData`.

- [performance_proxy_test.go](../../test/integration/handoff/performance_proxy_test.go) — tests.
  Types and interfaces: `meter`, `measuredRaw`, `measuredTransformed`.
  Functions and methods: `*meter.observe`, `*meter.snapshot`, `*measuredRaw.SubmitRawBlock`, `*measuredTransformed.SubmitTransformedBlock`.

- [performance_test.go](../../test/integration/handoff/performance_test.go) — tests.
  Functions and methods: `baselineEnvironment`, `baselinePort`, `scrape`, `baselineSeedCommand`, `TestPerformanceBaseline`, `addOperations`, `postgresSample`.

## `test/performance`

- [report.go](../../test/performance/report.go).
  Types and interfaces: `Scenario`, `WireSpec`, `SeedEvent`, `Seed`, `Sample`, `RPC`, `Report`.
  Functions and methods: `ReadScenario`, `Scenario.Validate`, `ExclusiveJSON`, `*Report.Accept`.

- [report_test.go](../../test/performance/report_test.go) — tests.
  Functions and methods: `TestNoIncompleteThroughput`, `TestScenarioGuards`.

## `test/validation`

- [final_test.go](../../test/validation/final_test.go) — tests.
  Functions and methods: `TestFinalValidationPhaseIsolation`.

## `tools/mainnet`

- [completion.go](../../tools/mainnet/completion.go).
  Functions and methods: `completionBounds`, `verifyCompletion`.

- [completion_test.go](../../tools/mainnet/completion_test.go) — tests.
  Functions and methods: `TestLiveE2EGuards`, `TestCompletionCommandsUseOwnRolesAndNoRPC`, `TestE2EAcquisitionFailureIsNotRetried`.

- [main.go](../../tools/mainnet/main.go).
  Types and interfaces: `executor`.
  Functions and methods: `main`, `start`, `initialize`, `readConfig`, `validate`, `environment`, `run`, `execute`, `containsArg`.
  Package values: `composeFile`, `defaults`.

- [main_test.go](../../tools/mainnet/main_test.go) — tests.
  Functions and methods: `configuration`, `TestPrivateInitialization`, `TestIsolationAndExplicitAcquisition`, `TestRefuseForeignVolumeAndRedactFailures`.

- [performance.go](../../tools/mainnet/performance.go).
  Functions and methods: `performanceKey`, `validatePerformance`.

- [performance_test.go](../../tools/mainnet/performance_test.go) — tests.
  Functions and methods: `TestPrivatePerformanceSettings`.

- [recovery.go](../../tools/mainnet/recovery.go).
  Types and interfaces: `recoveryRequest`.
  Functions and methods: `parseRecovery`, `operationID`, `recoveryArgs`.

- [recovery_test.go](../../tools/mainnet/recovery_test.go) — tests.
  Functions and methods: `TestRecoveryCommandsOffline`, `TestRestartNeverAcquiresAndRefusesActiveRun`.

## `tools/probe`

- [main.go](../../tools/probe/main.go).
  Functions and methods: `main`.
