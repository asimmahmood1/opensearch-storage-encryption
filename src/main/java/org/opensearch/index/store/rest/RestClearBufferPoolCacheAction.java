/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.rest;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

/**
 * Rest action to clear the buffer pool cache safely.
 * Only clears cache entries that are not currently in use (refCount == 1).
 * 
 * Route:
 * - POST/_plugins/_opensearch_storage_encryption/_flush_bufferpool
 */
public class RestClearBufferPoolCacheAction extends BaseRestHandler {
    private volatile boolean enabled = false;
    private static final Logger LOGGER = LogManager.getLogger(RestClearBufferPoolCacheAction.class);

    public RestClearBufferPoolCacheAction(ClusterSettings clusterSettings) {
        this.enabled = CryptoDirectoryFactory.BUFFERPOOL_FLUSH_ENABLED_SETTING.getDefault(Settings.EMPTY);
        clusterSettings.addSettingsUpdateConsumer(
            CryptoDirectoryFactory.BUFFERPOOL_FLUSH_ENABLED_SETTING,
            value -> this.enabled = value
        );
    }

    @Override
    public String getName() {
        return "clear_bufferpool_cache_action";
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_plugins/_opensearch_storage_encryption/_flush_bufferpool"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint().lfAtEnd();

        if (!enabled) {
            builder.startObject();
            builder.field("acknowledged", false);
            builder.field("error", "Buffer pool flush is disabled. Set plugins.crypto.bufferpool_flush.enabled to true.");
            builder.endObject();
            return channel -> { channel.sendResponse(new BytesRestResponse(RestStatus.FORBIDDEN, builder)); };
        }
        
        BlockCache<?> cache = CryptoDirectoryFactory.getSharedBlockCache();

        if (cache == null) {
            builder.startObject();
            builder.field("acknowledged", false);
            builder.field("error", "No buffer pool cache available. Ensure cryptofs indices exist.");
            builder.endObject();
            return channel -> { channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, builder)); };
        }

        long initialSize = cache.getCacheSize();

        // Clear entire cache safely
        cache.clearSafely();

        long finalSize = cache.getCacheSize();
        long cleared = initialSize - finalSize;
        String stats = cache.cacheStats();

        builder.startObject();
        builder.field("acknowledged", true);
        builder.field("entries_cleared", cleared);
        builder.field("initial_size", initialSize);
        builder.field("final_size", finalSize);
        builder.field("cache_stats", stats);
        builder.endObject();

        LOGGER.info("Buffer pool cache cleared safely, stats={}", stats);
        return channel -> { channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder)); };
    }
}