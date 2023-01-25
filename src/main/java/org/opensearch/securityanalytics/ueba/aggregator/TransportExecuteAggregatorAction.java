package org.opensearch.securityanalytics.ueba.aggregator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.securityanalytics.settings.SecurityAnalyticsSettings;
import org.opensearch.securityanalytics.transport.SecureTransportAction;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportExecuteAggregatorAction extends HandledTransportAction<ExecuteAggregatorRequest, ExecuteAggregatorResponse> implements SecureTransportAction {
    private final Client client;

    private final NamedXContentRegistry xContentRegistry;

    private final ClusterService clusterService;

    private final AggregatorService aggregatorService;

    private final Settings settings;

    private volatile Boolean filterByEnabled;

    private static final Logger log = LogManager.getLogger(TransportExecuteAggregatorAction.class);


    @Inject
    public TransportExecuteAggregatorAction(TransportService transportService, ActionFilters actionFilters, AggregatorService aggregatorService, ClusterService clusterService, NamedXContentRegistry xContentRegistry, Client client, Settings settings) {
        super(ExecuteAggregatorAction.NAME, transportService, actionFilters, ExecuteAggregatorRequest::new);
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.aggregatorService = aggregatorService;
        this.clusterService = clusterService;
        this.settings = settings;
        this.filterByEnabled = SecurityAnalyticsSettings.FILTER_BY_BACKEND_ROLES.get(this.settings);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(SecurityAnalyticsSettings.FILTER_BY_BACKEND_ROLES, this::setFilterByEnabled);
    }

    @Override
    protected void doExecute(Task task, ExecuteAggregatorRequest request, ActionListener<ExecuteAggregatorResponse> actionListener) {
        log.debug("Executing aggregator request: " + request);

        GetUebaAggregatorRequest getUebaAggregatorRequest = new GetUebaAggregatorRequest(request.getAggregatorId(), request.getVersion());

        client.execute(GetUebaAggregatorAction.INSTANCE, getUebaAggregatorRequest, new ActionListener<>() {

            @Override
            public void onResponse(GetUebaAggregatorResponse getUebaAggregatorResponse) {
                UebaAggregator aggregator = getUebaAggregatorResponse.getAggregator();
                aggregatorService.execute(aggregator, actionListener);
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
