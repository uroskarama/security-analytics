package org.opensearch.securityanalytics.ueba.aggregator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Objects;

public class AggregatorIndices {

    private static final Logger log = LogManager.getLogger(AggregatorIndices.class);

    private final AdminClient client;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    public AggregatorIndices(AdminClient client, ClusterService clusterService, ThreadPool threadPool) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    public static String aggregatorMappings() throws IOException {
        return new String(Objects.requireNonNull(AggregatorIndices.class.getClassLoader().getResourceAsStream("mappings/aggregators.json")).readAllBytes(), Charset.defaultCharset());
    }

    public void initAggregatorIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        if (!aggregatorIndexExists()) {
            CreateIndexRequest indexRequest = new CreateIndexRequest(UebaAggregator.aggregatorsIndex())
                    .mapping(aggregatorMappings())
                    .settings(Settings.builder().put("index.hidden", true).build());
            client.indices().create(indexRequest, actionListener);
        }
    }

    public boolean aggregatorIndexExists() {
        ClusterState clusterState = clusterService.state();
        return clusterState.getRoutingTable().hasIndex(UebaAggregator.aggregatorsIndex());
    }

    public ClusterIndexHealth aggregatorIndexHealth() {
        ClusterIndexHealth indexHealth = null;

        if (aggregatorIndexExists()) {
            IndexRoutingTable indexRoutingTable = clusterService.state().routingTable().index(UebaAggregator.aggregatorsIndex());
            IndexMetadata indexMetadata = clusterService.state().metadata().index(UebaAggregator.aggregatorsIndex());

            indexHealth = new ClusterIndexHealth(indexMetadata, indexRoutingTable);
        }
        return indexHealth;
    }

    public void indexAggregator(Aggregator aggregator){
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }
}
