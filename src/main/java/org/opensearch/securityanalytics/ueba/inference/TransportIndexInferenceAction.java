/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.*;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.securityanalytics.transport.SecureTransportAction;
import org.opensearch.securityanalytics.ueba.core.UEBAJobIndices;
import org.opensearch.securityanalytics.ueba.core.UEBAJobParameter;
import org.opensearch.securityanalytics.util.SecurityAnalyticsException;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;

public class TransportIndexInferenceAction extends HandledTransportAction<IndexEntityInferenceRequest, IndexEntityInferenceResponse> implements SecureTransportAction {

    public static final String PLUGIN_OWNER_FIELD = "security_analytics";
    private static final Logger log = LogManager.getLogger(TransportIndexInferenceAction.class);

    private final Client client;

    private final NamedXContentRegistry xContentRegistry;

    private final ClusterService clusterService;

    private final Settings settings;

    private final NamedWriteableRegistry namedWriteableRegistry;
    private final UEBAJobIndices jobIndices;

    @Inject
    public TransportIndexInferenceAction(TransportService transportService,
                                         Client client,
                                         ActionFilters actionFilters,
                                         NamedXContentRegistry xContentRegistry,
                                         UEBAJobIndices jobIndices,
                                         ClusterService clusterService,
                                         Settings settings,
                                         NamedWriteableRegistry namedWriteableRegistry) {
        super(IndexEntityInferenceAction.NAME, transportService, actionFilters, IndexEntityInferenceRequest::new);
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.jobIndices = jobIndices;
        this.clusterService = clusterService;
        this.settings = settings;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    protected void doExecute(Task task, IndexEntityInferenceRequest request, ActionListener<IndexEntityInferenceResponse> listener) {

        try {
            validateRequest(request);

            lazyCreateAndIndex(request, listener);

        } catch (IOException e){
            listener.onFailure(e);
        } catch (SecurityAnalyticsException e) {
            listener.onFailure(e);
        }
    }

    private void validateRequest(IndexEntityInferenceRequest request){
        XContentParser xcp;
        try {
            ActionRequestValidationException requestValidationException = request.validate();

            if (requestValidationException != null)
                throw new SecurityAnalyticsException("Request is not valid.", RestStatus.BAD_REQUEST, requestValidationException);

            xcp = XContentType.JSON.xContent().createParser(
                    xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE, request.getEntityInference().getSearchRequestString());

            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(xcp);
            SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder);
            ActionRequestValidationException searchStringValidationException = searchRequest.validate();

            if (searchStringValidationException != null)
                throw new SecurityAnalyticsException("Request search string is not valid.", RestStatus.BAD_REQUEST, searchStringValidationException);

        } catch (IOException e) {
            log.error("Exception while validating index inference job request. ", e);
            throw new SecurityAnalyticsException("Request string is not valid.", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private void lazyCreateAndIndex(IndexEntityInferenceRequest request, ActionListener<IndexEntityInferenceResponse> listener) throws IOException {
        EntityInference inference = request.getEntityInference();

        if (!jobIndices.jobIndexExists())
        {
            jobIndices.initJobIndex(new CreateIndexListener(inference, listener));

        } else {
            indexInference(inference, listener);

        }
    }

    private void indexInference(EntityInference inference, ActionListener<IndexEntityInferenceResponse> listener) throws SecurityAnalyticsException, IOException {
        IndexRequest indexRequest = new IndexRequest();

        indexRequest.index(UEBAJobParameter.jobParameterIndex())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(inference.toXContent(XContentFactory.jsonBuilder(), null))
                .id(inference.getId());

        client.index(indexRequest, new IndexListener(inference, listener));
    }

    private class IndexListener implements ActionListener<IndexResponse> {

        private final EntityInference inference;
        private final ActionListener<IndexEntityInferenceResponse> listener;

        IndexListener(EntityInference inference, ActionListener<IndexEntityInferenceResponse> listener) { this.inference = inference; this.listener = listener; }

        @Override
        public void onResponse(IndexResponse indexResponse) {
            listener.onResponse(new IndexEntityInferenceResponse(inference.getId(), 0L, indexResponse.status(), inference));   // TODO: Implement versioning.
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(new SecurityAnalyticsException("Unable to index inference job.", RestStatus.INTERNAL_SERVER_ERROR, e));
        }
    }

    private class CreateIndexListener implements ActionListener<CreateIndexResponse> {
        private final EntityInference inference;
        private final ActionListener<IndexEntityInferenceResponse> listener;

        CreateIndexListener(EntityInference inference, ActionListener<IndexEntityInferenceResponse> listener) { this.inference = inference; this.listener = listener; }

        @Override
        public void onResponse(CreateIndexResponse createIndexResponse) {
            if (!createIndexResponse.isAcknowledged()) {
                onFailure(null);
                return;
            }

            try {
                indexInference(inference, listener);

            } catch (IOException e) {
                listener.onFailure(new SecurityAnalyticsException("Unable to index inference job.", RestStatus.INTERNAL_SERVER_ERROR, e));
            }
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(new SecurityAnalyticsException("Unable to create job index.", RestStatus.INTERNAL_SERVER_ERROR, e));
        }
    }

}