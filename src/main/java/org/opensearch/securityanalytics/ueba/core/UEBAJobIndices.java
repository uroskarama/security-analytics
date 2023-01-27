package org.opensearch.securityanalytics.ueba.core;

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
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Objects;

public class UEBAJobIndices {

    private static final Logger log = LogManager.getLogger(UEBAJobIndices.class);

    private final AdminClient client;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    public UEBAJobIndices(AdminClient client, ClusterService clusterService, ThreadPool threadPool) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    public static String jobMappings() throws IOException {
        return new String(Objects.requireNonNull(UEBAJobIndices.class.getClassLoader().getResourceAsStream("mappings/ueba_job.json")).readAllBytes(), Charset.defaultCharset());
    }

    public void initJobIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        if (!jobIndexExists()) {
            CreateIndexRequest indexRequest = new CreateIndexRequest(UEBAJobParameter.jobParameterIndex())
                    .mapping(jobMappings())
                    .settings(Settings.builder().put("index.hidden", true).build());
            client.indices().create(indexRequest, actionListener);
        }
    }

    public boolean jobIndexExists() {
        ClusterState clusterState = clusterService.state();
        return clusterState.getRoutingTable().hasIndex(UEBAJobParameter.jobParameterIndex());
    }

    public ClusterIndexHealth jobIndexHealth() {
        ClusterIndexHealth indexHealth = null;

        if (jobIndexExists()) {
            IndexRoutingTable indexRoutingTable = clusterService.state().routingTable().index(UEBAJobParameter.jobParameterIndex());
            IndexMetadata indexMetadata = clusterService.state().metadata().index(UEBAJobParameter.jobParameterIndex());

            indexHealth = new ClusterIndexHealth(indexMetadata, indexRoutingTable);
        }
        return indexHealth;
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }
}
