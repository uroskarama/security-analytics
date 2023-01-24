/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.aggregator;

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
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.*;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.securityanalytics.transport.SecureTransportAction;
import org.opensearch.securityanalytics.util.SecurityAnalyticsException;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;

public class TransportIndexUebaAggregatorAction extends HandledTransportAction<IndexUebaAggregatorRequest, IndexUebaAggregatorResponse> implements SecureTransportAction {

    public static final String PLUGIN_OWNER_FIELD = "security_analytics";
    private static final Logger log = LogManager.getLogger(TransportIndexUebaAggregatorAction.class);

    private final Client client;

    private final NamedXContentRegistry xContentRegistry;

    private final ClusterService clusterService;

    private final Settings settings;

    private final NamedWriteableRegistry namedWriteableRegistry;
    private final AggregatorIndices aggregatorIndices;
    private final AggregatorService aggregatorService;

    @Inject
    public TransportIndexUebaAggregatorAction(TransportService transportService,
                                              Client client,
                                              ActionFilters actionFilters,
                                              NamedXContentRegistry xContentRegistry,
                                              AggregatorIndices aggregatorIndices,
                                              AggregatorService aggregatorService,
                                              ClusterService clusterService,
                                              Settings settings,
                                              NamedWriteableRegistry namedWriteableRegistry) {
        super(IndexUebaAggregatorAction.NAME, transportService, actionFilters, IndexUebaAggregatorRequest::new);
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.aggregatorIndices = aggregatorIndices;
        this.aggregatorService = aggregatorService;
        this.clusterService = clusterService;
        this.settings = settings;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    protected void doExecute(Task task, IndexUebaAggregatorRequest request, ActionListener<IndexUebaAggregatorResponse> listener) {

        try {
            validateRequest(request);

            lazyCreateAndIndex(request, listener);

        } catch (IOException e){
            listener.onFailure(e);
        } catch (SecurityAnalyticsException e) {
            listener.onFailure(e);
        }
    }

    private void validateRequest(IndexUebaAggregatorRequest request){
        XContentParser xcp;
        try {
            ActionRequestValidationException requestValidationException = request.validate();

            if (requestValidationException != null)
                throw new SecurityAnalyticsException("Request is not valid.", RestStatus.BAD_REQUEST, requestValidationException);

            xcp = XContentType.JSON.xContent().createParser(
                    xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE, request.getUebaAggregator().getSearchRequestString());

            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(xcp);
            SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder);
            ActionRequestValidationException searchStringValidationException = searchRequest.validate();

            if (searchStringValidationException != null)
                throw new SecurityAnalyticsException("Request search string is not valid.", RestStatus.BAD_REQUEST, searchStringValidationException);

        } catch (IOException e) {
            log.error("Exception while validating index aggregator request. ", e);
            throw new SecurityAnalyticsException("Request string is not valid.", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private void lazyCreateAndIndex(IndexUebaAggregatorRequest request, ActionListener<IndexUebaAggregatorResponse> listener) throws IOException {
        UebaAggregator aggregator = request.getUebaAggregator();

        if (!aggregatorIndices.aggregatorIndexExists())
        {
            aggregatorIndices.initAggregatorIndex(new CreateIndexListener(aggregator, listener));

        } else {
            indexAggregator(aggregator, listener);

        }
    }

    private void indexAggregator(UebaAggregator aggregator, ActionListener<IndexUebaAggregatorResponse> listener) throws SecurityAnalyticsException, IOException {
        IndexRequest indexRequest = new IndexRequest();

        indexRequest.index(UebaAggregator.aggregatorsIndex())
                .source(aggregator.toXContent(XContentFactory.jsonBuilder(), null))
                .id(aggregator.getId());

        client.index(indexRequest, new IndexListener(aggregator, listener));
    }

    private class IndexListener implements ActionListener<IndexResponse> {

        private final UebaAggregator aggregator;
        private final ActionListener<IndexUebaAggregatorResponse> listener;

        IndexListener(UebaAggregator aggregator, ActionListener<IndexUebaAggregatorResponse> listener) { this.aggregator = aggregator; this.listener = listener; }

        @Override
        public void onResponse(IndexResponse indexResponse) {
            listener.onResponse(new IndexUebaAggregatorResponse(aggregator.getId(), 0L, indexResponse.status(), aggregator));   // TODO: Implement versioning.
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(new SecurityAnalyticsException("Unable to index aggregator.", RestStatus.INTERNAL_SERVER_ERROR, e));
        }
    }

    private class CreateIndexListener implements ActionListener<CreateIndexResponse> {
        private final UebaAggregator aggregator;
        private final ActionListener<IndexUebaAggregatorResponse> listener;

        CreateIndexListener(UebaAggregator aggregator, ActionListener<IndexUebaAggregatorResponse> listener) { this.aggregator = aggregator; this.listener = listener; }

        @Override
        public void onResponse(CreateIndexResponse createIndexResponse) {
            if (!createIndexResponse.isAcknowledged()) {
                onFailure(null);
                return;
            }

            try {
                indexAggregator(aggregator, listener);

            } catch (IOException e) {
                listener.onFailure(new SecurityAnalyticsException("Unable to index aggregator.", RestStatus.INTERNAL_SERVER_ERROR, e));
            }
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(new SecurityAnalyticsException("Unable to create aggregator index.", RestStatus.INTERNAL_SERVER_ERROR, e));
        }
    }

}