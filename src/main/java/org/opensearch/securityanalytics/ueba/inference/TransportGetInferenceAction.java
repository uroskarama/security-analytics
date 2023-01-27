package org.opensearch.securityanalytics.ueba.inference;

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
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestStatus;
import org.opensearch.securityanalytics.settings.SecurityAnalyticsSettings;
import org.opensearch.securityanalytics.transport.SecureTransportAction;
import org.opensearch.securityanalytics.ueba.core.UEBAJobIndices;
import org.opensearch.securityanalytics.ueba.core.UEBAJobParameter;
import org.opensearch.securityanalytics.util.SecurityAnalyticsException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.opensearch.rest.RestStatus.OK;

public class TransportGetInferenceAction extends HandledTransportAction<GetInferenceRequest, GetInferenceResponse> implements SecureTransportAction {

    private final Client client;

    private final NamedXContentRegistry xContentRegistry;

    private final org.opensearch.securityanalytics.ueba.core.UEBAJobIndices UEBAJobIndices;

    private final ClusterService clusterService;

    private final Settings settings;

    private final ThreadPool threadPool;

    private volatile Boolean filterByEnabled;

    private static final Logger log = LogManager.getLogger(TransportGetInferenceAction.class);


    @Inject
    public TransportGetInferenceAction(TransportService transportService, ActionFilters actionFilters, UEBAJobIndices UEBAJobIndices, ClusterService clusterService, NamedXContentRegistry xContentRegistry, Client client, Settings settings) {
        super(GetInferenceAction.NAME, transportService, actionFilters, GetInferenceRequest::new);
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.UEBAJobIndices = UEBAJobIndices;
        this.clusterService = clusterService;
        this.threadPool = this.UEBAJobIndices.getThreadPool();
        this.settings = settings;
        this.filterByEnabled = SecurityAnalyticsSettings.FILTER_BY_BACKEND_ROLES.get(this.settings);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(SecurityAnalyticsSettings.FILTER_BY_BACKEND_ROLES, this::setFilterByEnabled);
    }

    @Override
    protected void doExecute(Task task, GetInferenceRequest request, ActionListener<GetInferenceResponse> actionListener) {
        GetRequest getRequest = new GetRequest(UEBAJobParameter.jobParameterIndex(), request.getInferenceId());

        client.get(getRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse response) {
                try {
                    if (!response.isExists() || response.isSourceEmpty()) {
                        actionListener.onFailure(SecurityAnalyticsException.wrap(new OpenSearchStatusException("Inference job " + request.getInferenceId() + " not found.", RestStatus.NOT_FOUND)));
                        return;
                    }

                    XContentParser xcp = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString());
                    xcp.nextToken();

                    EntityInference inference = EntityInference.parse(xcp, request.getInferenceId(), null, null);

                    assert inference != null;

                    actionListener.onResponse(new GetInferenceResponse(inference.getId(), 1L, OK, inference));
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

