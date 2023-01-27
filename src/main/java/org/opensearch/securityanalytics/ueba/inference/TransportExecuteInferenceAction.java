package org.opensearch.securityanalytics.ueba.inference;

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

public class TransportExecuteInferenceAction extends HandledTransportAction<ExecuteInferenceRequest, ExecuteInferenceResponse> implements SecureTransportAction {
    private final Client client;

    private final NamedXContentRegistry xContentRegistry;

    private final ClusterService clusterService;

    private final InferenceService inferenceService;

    private final Settings settings;

    private volatile Boolean filterByEnabled;

    private static final Logger log = LogManager.getLogger(TransportExecuteInferenceAction.class);


    @Inject
    public TransportExecuteInferenceAction(TransportService transportService, ActionFilters actionFilters, InferenceService inferenceService, ClusterService clusterService, NamedXContentRegistry xContentRegistry, Client client, Settings settings) {
        super(ExecuteInferenceAction.NAME, transportService, actionFilters, ExecuteInferenceRequest::new);
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.inferenceService = inferenceService;
        this.clusterService = clusterService;
        this.settings = settings;
        this.filterByEnabled = SecurityAnalyticsSettings.FILTER_BY_BACKEND_ROLES.get(this.settings);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(SecurityAnalyticsSettings.FILTER_BY_BACKEND_ROLES, this::setFilterByEnabled);
    }

    @Override
    protected void doExecute(Task task, ExecuteInferenceRequest request, ActionListener<ExecuteInferenceResponse> actionListener) {
        log.debug("Executing inference request: " + request);

        GetInferenceRequest getInferenceRequest = new GetInferenceRequest(request.getInferenceId(), request.getVersion());

        client.execute(GetInferenceAction.INSTANCE, getInferenceRequest, new ActionListener<>() {

            @Override
            public void onResponse(GetInferenceResponse getInferenceResponse) {
                EntityInference inference = getInferenceResponse.getInference();
                inferenceService.execute(inference, actionListener);
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
