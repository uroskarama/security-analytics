package org.opensearch.securityanalytics.ueba.aggregator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.*;
import org.opensearch.rest.RestStatus;
import org.opensearch.securityanalytics.settings.SecurityAnalyticsSettings;
import org.opensearch.securityanalytics.transport.SecureTransportAction;
import org.opensearch.securityanalytics.transport.TransportGetDetectorAction;
import org.opensearch.securityanalytics.util.SecurityAnalyticsException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.opensearch.rest.RestStatus.OK;

public class TransportGetUebaAggregatorAction extends HandledTransportAction<GetUebaAggregatorRequest, GetUebaAggregatorResponse> implements SecureTransportAction {

    private final Client client;

    private final NamedXContentRegistry xContentRegistry;

    private final AggregatorIndices aggregatorIndices;

    private final ClusterService clusterService;

    private final Settings settings;

    private final ThreadPool threadPool;

    private volatile Boolean filterByEnabled;

    private static final Logger log = LogManager.getLogger(TransportGetDetectorAction.class);


    @Inject
    public TransportGetUebaAggregatorAction(TransportService transportService, ActionFilters actionFilters, AggregatorIndices aggregatorIndices, ClusterService clusterService, NamedXContentRegistry xContentRegistry, Client client, Settings settings) {
        super(GetUebaAggregatorAction.NAME, transportService, actionFilters, GetUebaAggregatorRequest::new);
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.aggregatorIndices = aggregatorIndices;
        this.clusterService = clusterService;
        this.threadPool = this.aggregatorIndices.getThreadPool();
        this.settings = settings;
        this.filterByEnabled = SecurityAnalyticsSettings.FILTER_BY_BACKEND_ROLES.get(this.settings);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(SecurityAnalyticsSettings.FILTER_BY_BACKEND_ROLES, this::setFilterByEnabled);
    }

    @Override
    protected void doExecute(Task task, GetUebaAggregatorRequest request, ActionListener<GetUebaAggregatorResponse> actionListener) {
        GetRequest getRequest = new GetRequest(UebaAggregator.aggregatorsIndex(), request.getAggregatorId());

        client.get(getRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse response) {
                try {
                    if (!response.isExists() || response.isSourceEmpty()) {
                        actionListener.onFailure(SecurityAnalyticsException.wrap(new OpenSearchStatusException("Aggregator " + request.getAggregatorId() + " not found.", RestStatus.NOT_FOUND)));
                        return;
                    }

                    XContentParser xcp = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString());
                    xcp.nextToken();

                    UebaAggregator aggregator = UebaAggregator.parse(xcp, request.getAggregatorId(), null, null);

                    assert aggregator != null;


                    actionListener.onResponse(new GetUebaAggregatorResponse(aggregator.getId(), 1L, OK, aggregator));
                } catch (IOException ex) {

                    actionListener.onFailure(ex);
                }
            }

            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        });
    }

    private void setFilterByEnabled(boolean filterByEnabled) {
        this.filterByEnabled = filterByEnabled;
    }

}

