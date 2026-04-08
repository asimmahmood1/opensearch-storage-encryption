/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import com.amazonaws.juno.settings.JunoSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.store.action.GetIndexCountForKeyAction;
import org.opensearch.index.store.action.TransportGetIndexCountForKeyAction;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.key.MasterKeyHealthMonitor;
import org.opensearch.index.store.key.NodeLevelKeyCache;
import org.opensearch.index.store.key.ShardKeyResolverRegistry;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.metrics.BufferPoolMetricsProviderImpl;
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;
import org.opensearch.index.store.pool.PoolSizeCalculator;
import org.opensearch.index.store.rest.RestGetIndexCountForKeyAction;
import org.opensearch.index.store.rest.RestRegisterCryptoAction;
import org.opensearch.index.store.rest.RestUnregisterCryptoAction;
import org.opensearch.index.store.rest.RestClearBufferPoolCacheAction;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryAwarePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;
import org.opensearch.index.store.hll.WorkingSetEstimatorScheduler;
import org.opensearch.index.store.rest.RestCacheStatsAction;
import org.opensearch.index.store.iouring.core.IoUringConfig;
import org.opensearch.index.store.iouring.core.IoUringMetrics;
import org.opensearch.index.store.iouring.core.IoUringRing;

/**
 * A plugin that enables index level encryption and decryption.
 */
public class CryptoDirectoryPlugin extends Plugin implements IndexStorePlugin, EnginePlugin, TelemetryAwarePlugin, ActionPlugin {
    private static final Logger log = LogManager.getLogger(CryptoDirectoryPlugin.class);

    /**
     * Setting key for enabling the crypto plugin.
     */
    public static final String CRYPTO_PLUGIN_ENABLED = "plugins.crypto.enabled";

    public static final String CRYPTO_PLUGIN_THREADPOOL_PREFETCH = "crypto_plugin_prefetch_threadpool";

    /**
     * Setting for controlling whether the crypto plugin is enabled.
     */
    public static final Setting<Boolean> CRYPTO_PLUGIN_ENABLED_SETTING = Setting
        .boolSetting(CRYPTO_PLUGIN_ENABLED, true, Setting.Property.NodeScope, Setting.Property.Filtered, Setting.Property.Final);

    private NodeEnvironment nodeEnvironment;
    private final boolean enabled;

    // Static storage for remote store parameters (accessible by CryptoEngineFactory)
    private static Supplier<RepositoriesService> repositoriesServiceSupplier;
    private static RemoteStoreSettings remoteStoreSettings;

    /**
     * Constructor with settings.
     * @param settings OpenSearch node settings
     */
    public CryptoDirectoryPlugin(Settings settings) {
        super();
        this.enabled = settings.getAsBoolean(CRYPTO_PLUGIN_ENABLED, true);

        if (enabled) {
            log.info("OpenSearch Crypto Directory Plugin is enabled and ready for encryption operations");
        } else {
            log
                .warn(
                    "OpenSearch Crypto Directory Plugin installed but disabled. "
                        + "No encryption/decryption will be performed. "
                        + "To enable encryption, set '{}' to true in opensearch.yml",
                    CRYPTO_PLUGIN_ENABLED
                );
        }
        log.info("bufferpool prefetch enabled: {}", JunoSettings.STORAGE_PREFETCH_ENABLED.get());
    }

    /**
     * Check if the plugin is disabled.
     * @return true if the plugin is disabled, false otherwise
     */
    public boolean isDisabled() {
        return !enabled;
    }

    /**
     * Get the RepositoriesService supplier for remote store operations.
     * @return the repositories service supplier, or null if not initialized
     */
    public static Supplier<RepositoriesService> getRepositoriesServiceSupplier() {
        return repositoriesServiceSupplier;
    }

    /**
     * Get the RemoteStoreSettings for remote store operations.
     * @return the remote store settings, or null if not initialized
     */
    public static RemoteStoreSettings getRemoteStoreSettings() {
        return remoteStoreSettings;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = Arrays
            .asList(
                CRYPTO_PLUGIN_ENABLED_SETTING,
                CryptoDirectoryFactory.INDEX_KEY_PROVIDER_SETTING,
                CryptoDirectoryFactory.INDEX_KMS_ARN_SETTING,
                CryptoDirectoryFactory.INDEX_KMS_ENC_CTX_SETTING,
                CryptoDirectoryFactory.NODE_KEY_REFRESH_INTERVAL_SETTING,
                CryptoDirectoryFactory.NODE_KEY_EXPIRY_INTERVAL_SETTING,
                PoolSizeCalculator.NODE_POOL_SIZE_PERCENTAGE_SETTING,
                PoolSizeCalculator.NODE_CACHE_TO_POOL_RATIO_SETTING,
                PoolSizeCalculator.NODE_WARMUP_PERCENTAGE_SETTING,
                CryptoDirectoryFactory.BUFFERPOOL_FLUSH_ENABLED_SETTING
            );
        return settings;
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        if (isDisabled()) {
            log.debug("Crypto Directory Plugin is disabled. No directory factories will be registered.");
            return Collections.emptyMap();
        }
        log.debug("Crypto Directory Plugin is enabled. Registering cryptofs directory factory.");
        return Collections.singletonMap(CryptoDirectoryFactory.STORE_TYPE, new CryptoDirectoryFactory());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (isDisabled()) {
            return Optional.empty();
        }

        // Only provide our custom engine factory for cryptofs indices
        // if (CryptoDirectoryFactory.STORE_TYPE.equals(indexSettings.getValue(IndexModule.INDEX_STORE_TYPE_SETTING))) {
        //     return Optional.of(new CryptoEngineFactory());
        // }
        return Optional.empty();
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver expressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        MetricsRegistry metricsRegistry
    ) {
        if (isDisabled()) {
            log.debug("Crypto Directory Plugin is disabled. Skipping component initialization.");
            return Collections.emptyList();
        }

        this.nodeEnvironment = nodeEnvironment;

        // Store remote store parameters for CryptoEngineFactory to access
        CryptoDirectoryPlugin.repositoriesServiceSupplier = repositoriesServiceSupplier;
        // Create RemoteStoreSettings with node settings and cluster settings
        CryptoDirectoryPlugin.remoteStoreSettings = new RemoteStoreSettings(environment.settings(), clusterService.getClusterSettings());

        // Initialize StaticConfigs with the DynamicConfig block size value
        StaticConfigs.init(JunoSettings.JUNO_STORAGE_ENCRYPTION_BLOCK_SIZE_SETTING.get());

        // Set cluster service for accessing cluster metadata (e.g., repository settings)
        CryptoDirectoryFactory.setClusterService(clusterService);

        // Initialize health monitor first (creates monitor)
        MasterKeyHealthMonitor.initialize(environment.settings(), client, clusterService);

        // Initialize cache second (depends on health monitor reference)
        NodeLevelKeyCache.initialize(environment.settings(), MasterKeyHealthMonitor.getInstance());

        // Start health monitoring now that everything is initialized
        MasterKeyHealthMonitor.start();

        // Pool resources are lazily initialized on first cryptofs shard creation
        // This prevents allocation on dedicated master nodes which never create shards
        CryptoDirectoryFactory.setNodeSettings(environment.settings());
        CryptoDirectoryFactory.setThreadPool(threadPool);
        CryptoMetricsService.initialize(metricsRegistry);

        // Initialize I/O backend setting and register dynamic update listener
        CryptoDirectoryFactory.initializeIOBackendSetting(clusterService);

        // Create HLL working set estimator scheduler (starts when enabled via dynamic config)
        WorkingSetEstimatorScheduler hllScheduler =
            new WorkingSetEstimatorScheduler(threadPool);

        // Register buffer pool metrics provider with JunoSearchWorker
        BufferPoolMetricsProviderImpl metricsProvider =
            new BufferPoolMetricsProviderImpl(hllScheduler);
        com.amazonaws.juno.metric.bufferpool.BufferPoolMetricsRegistry.register(metricsProvider);

        // Initialize io_uring and register metrics provider
        initializeIoUring(environment.settings());

        log.info("ILE DEBUG: Plugin initialized!");

        return Collections.singletonList(hllScheduler);
    }

    /**
     * Initializes io_uring ring and registers the metrics provider with JunoSearchWorker.
     */
    private void initializeIoUring(Settings settings) {
        boolean ioUringEnabled = com.amazonaws.juno.settings.JunoSettings.IOURING_ENABLED.get();
        IoUringRing.setEnabled(ioUringEnabled);

        if (!ioUringEnabled) {
            log.info("io_uring is disabled via setting [juno.iouring.enabled]");
            return;
        }

        if (!IoUringRing.isAvailable()) {
            log.warn("io_uring is enabled but not available on this system, skipping initialization");
            IoUringRing.setEnabled(false);
            return;
        }

        try {
            String storageTypeStr = com.amazonaws.juno.settings.JunoSettings.IOURING_STORAGE_TYPE.get();
            IoUringConfig.StorageType storageType;
            try {
                storageType = IoUringConfig.StorageType.valueOf(storageTypeStr.toUpperCase());
            } catch (IllegalArgumentException e) {
                log.warn("Unknown io_uring storage type [{}], falling back to GENERIC", storageTypeStr);
                storageType = IoUringConfig.StorageType.GENERIC;
            }

            IoUringConfig config = IoUringConfig.builder()
                .ringSize(com.amazonaws.juno.settings.JunoSettings.IOURING_RING_SIZE.get())
                .maxInflightOps(com.amazonaws.juno.settings.JunoSettings.IOURING_MAX_INFLIGHT_OPS.get())
                .pollBackoffInitialNs(com.amazonaws.juno.settings.JunoSettings.IOURING_POLL_BACKOFF_INITIAL_NS.get())
                .pollBackoffMaxNs(com.amazonaws.juno.settings.JunoSettings.IOURING_POLL_BACKOFF_MAX_NS.get())
                .shutdownTimeoutMs(com.amazonaws.juno.settings.JunoSettings.IOURING_SHUTDOWN_TIMEOUT_MS.get())
                .storageType(storageType)
                .metricsEnabled(com.amazonaws.juno.settings.JunoSettings.IOURING_METRICS_ENABLED.get())
                .build();

            IoUringRing.initialize(config);
            log.info("io_uring initialized with config: {}", config);

            // Register metrics provider so NodeStatsCollector can read io_uring metrics
            com.amazonaws.juno.metric.iouring.IoUringMetricsRegistry.register(new IoUringMetricsProviderImpl());
        } catch (Exception e) {
            log.error("Failed to initialize io_uring, disabling", e);
            IoUringRing.setEnabled(false);
        }
    }

    /**
     * Bridges io_uring metrics to the registry interface consumed by NodeStatsCollector.
     */
    private static class IoUringMetricsProviderImpl implements com.amazonaws.juno.metric.iouring.IoUringMetricsProvider {
        private volatile IoUringMetrics.MetricsSnapshot cachedSnapshot;

        @Override
        public boolean takeSnapshot() {
            IoUringRing ring = IoUringRing.getInstanceOrNull();
            if (ring == null) {
                cachedSnapshot = null;
                return false;
            }
            cachedSnapshot = ring.takeMetricsSnapshot();
            return cachedSnapshot != null;
        }

        @Override public long getSuccessCount() { return cachedSnapshot != null ? cachedSnapshot.successCount() : 0; }
        @Override public long getFailureCount() { return cachedSnapshot != null ? cachedSnapshot.failureCount() : 0; }
        @Override public long getSubmissionCount() { return cachedSnapshot != null ? cachedSnapshot.submissionCount() : 0; }
        @Override public long getMinLatencyNs() { return cachedSnapshot != null ? cachedSnapshot.minLatencyNs() : 0; }
        @Override public long getMaxLatencyNs() { return cachedSnapshot != null ? cachedSnapshot.maxLatencyNs() : 0; }
        @Override public long getMeanLatencyNs() { return cachedSnapshot != null ? cachedSnapshot.meanLatencyNs() : 0; }
        @Override public long getQueueFullEvents() { return cachedSnapshot != null ? cachedSnapshot.queueFullEvents() : 0; }
        @Override public double getOperationsPerSecond() { return cachedSnapshot != null ? cachedSnapshot.operationsPerSecond() : 0.0; }
        @Override public double getSuccessRate() { return cachedSnapshot != null ? cachedSnapshot.successRate() : 0.0; }
        @Override public int getPendingOps() {
            IoUringRing ring = IoUringRing.getInstanceOrNull();
            return ring != null ? ring.getPendingCount() : 0;
        }
    }

    @Override
    public void close() {
        if (isDisabled()) {
            log.debug("Crypto Directory Plugin is disabled. No cleanup needed.");
            return;
        }

        MasterKeyHealthMonitor.shutdown();
        CryptoDirectoryFactory.closeSharedPool();
    }

    @Override
    public List<ActionHandler<?, ?>> getActions() {
        return Arrays.asList(new ActionHandler<>(GetIndexCountForKeyAction.INSTANCE, TransportGetIndexCountForKeyAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return Arrays.asList(
            new RestRegisterCryptoAction(),
            new RestUnregisterCryptoAction(),
            new RestGetIndexCountForKeyAction(),
            new RestClearBufferPoolCacheAction(clusterSettings),
            new RestCacheStatsAction()
        );
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (isDisabled()) {
            log
                .debug(
                    "Crypto Directory Plugin is disabled. Skipping index module initialization for index: {}",
                    indexModule.getIndex().getName()
                );
            return;
        }

        // Commenting below store-type validation since currently we do not have cryptofs as a store type setting in metadata service
        // and we are relying on cryptofs as a directoryFactory 
        // Settings indexSettings = indexModule.getSettings();
        // String storeType = indexSettings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey());

        // if (CryptoDirectoryFactory.STORE_TYPE.equals(storeType)) {
            // Validate crypto settings early at index creation time
            // CryptoIndexSettingsValidator.validate(indexSettings);
            indexModule.addIndexEventListener(new IndexEventListener() {
                /*
                 * Cache invalidation for closed shards is handled automatically
                 * by CryptoDirectIODirectory.close() when the directory is closed.
                 */
                @Override
                public void afterIndexRemoved(Index index, IndexSettings idxSettings, IndexRemovalReason reason) {
                    if (reason != IndexRemovalReason.DELETED) {
                        return;
                    }

                    BlockCache<?> cache = CryptoDirectoryFactory.getSharedBlockCache();
                    if (cache != null && nodeEnvironment != null) {
                        for (Path indexPath : nodeEnvironment.indexPaths(index)) {
                            cache.invalidateByPathPrefix(indexPath);
                        }
                    }

                    /*
                    * The resolvers should be removed only when the index is actually deleted (DELETED reason).
                    * We should NOT remove resolvers when shards are relocated (NO_LONGER_ASSIGNED) or during
                    * node restarts, as other nodes may still need the resolver for their shards.
                    * 
                    * This prevents race conditions during:
                    * - Shard relocation between nodes
                    * - Node restarts with replica recovery
                    * - Cluster topology changes
                    * */
                    int nShards = idxSettings.getNumberOfShards();
                    for (int i = 0; i < nShards; i++) {
                        ShardKeyResolverRegistry.removeResolver(index.getUUID(), i, index.getName());
                        NodeLevelKeyCache.getInstance().evict(index.getUUID(), i, index.getName());
                    }
                }
            });
        // }
    }
}
