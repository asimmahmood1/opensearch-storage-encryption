/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.rest;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

import java.util.List;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.index.store.block_cache.PrefetchTracker;
import org.opensearch.index.store.hll.WorkingSetEstimator;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

public class RestCacheStatsAction extends BaseRestHandler {
    private static final String ACTION_NAME = "cache_stats_action";
    private static final String ROUTE_PATH = "/_plugins/_opensearch_storage_encryption/_cache_stats";

    @Override
    public String getName() {
        return ACTION_NAME;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, ROUTE_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            try {
                String cacheStats = CryptoDirectoryFactory.getSharedCacheStats().orElse("Cache not initialized");
                WorkingSetEstimator wse = WorkingSetEstimator.getInstance();

                long estimate5min = wse.estimateCardinality(300L);
                long estimate10min = wse.estimateCardinality(600L);
                long estimate30min = wse.estimateCardinality(1800L);

                long raLoaded = wse.getReadAheadBlocksLoaded();
                long raAccessed = wse.getReadAheadBlocksAccessed();
                long raWasted = wse.getReadAheadBlocksWasted();
                double raHitRate = wse.getReadAheadHitRate();

                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.field("cache_stats", cacheStats);

                builder.startObject("working_set_estimates");
                builder.field("window_5min_blocks", estimate5min);
                builder.field("window_10min_blocks", estimate10min);
                builder.field("window_30min_blocks", estimate30min);
                builder.field("window_5min_size_mb", WorkingSetEstimator.blocksToMB(estimate5min));
                builder.field("window_10min_size_mb", WorkingSetEstimator.blocksToMB(estimate10min));
                builder.field("window_30min_size_mb", WorkingSetEstimator.blocksToMB(estimate30min));
                builder.endObject();

                builder.startObject("read_ahead_stats");
                builder.field("blocks_loaded", raLoaded);
                builder.field("blocks_accessed", raAccessed);
                builder.field("blocks_wasted", raWasted);
                builder.field("hit_rate_percent", String.format("%.2f", raHitRate));
                builder.field("size_loaded_mb", WorkingSetEstimator.blocksToMB(raLoaded));
                builder.field("size_accessed_mb", WorkingSetEstimator.blocksToMB(raAccessed));
                builder.field("size_wasted_mb", WorkingSetEstimator.blocksToMB(raWasted));
                builder.endObject();

                PrefetchTracker pt = CryptoDirectoryFactory.getSharedPrefetchTracker();
                if (pt != null) {
                    builder.startObject("prefetch_stats");
                    builder.field("lead_hits", pt.getLeadHits());
                    builder.field("lead_misses", pt.getLeadMisses());
                    builder.field("calls", pt.getCalls());
                    builder.field("blocks_requested", pt.getBlocksRequested());
                    builder.field("blocks_loaded", pt.getBlocksLoaded());
                    builder.field("blocks_deduped", pt.getBlocksDeduped());
                    builder.field("blocks_cache_hit", pt.getBlocksCacheHit());
                    builder.field("l1_hits", pt.getL1Hits());
                    builder.field("l1_misses", pt.getL1Misses());
                    builder.field("l1_promotions", pt.getL1Promotions());
                    builder.endObject();
                }

                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (Exception e) {
                channel.sendResponse(
                    new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "Error retrieving cache stats: " + e.getMessage())
                );
            }
        };
    }
}
