
package org.opensearch.securityanalytics.ueba.inference;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.*;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.xcontent.*;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.commons.alerting.util.IndexUtilsKt;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.securityanalytics.ueba.core.UEBAJobExecutionMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.securityanalytics.ueba.core.UEBAJobExecutionMetadata.ExecutionState.*;
import static org.opensearch.securityanalytics.ueba.inference.RestIndexInferenceAction.INFERENCE_ID;

public class InferenceService {

    private static final Logger log = LogManager.getLogger(InferenceService.class);

    private final Client client;

    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public InferenceService(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    public void execute(EntityInference inference, ActionListener<ExecuteInferenceResponse> listener){
        InferenceExecutionMetadata metadata = new InferenceExecutionMetadata(inference, QUERYING);

        EntityInferenceFSM eventAggregatorFSM = new EntityInferenceFSM(inference, listener);

        eventAggregatorFSM.start(metadata);
    }


    private class EntityInferenceFSM {

        private final ActionListener<ExecuteInferenceResponse> listener;

        private final EntityInference inference;

        private final RestClient restClient;

        EntityInferenceFSM(EntityInference inference, ActionListener<ExecuteInferenceResponse> listener) {
            this.inference = inference;
            this.listener = listener;

            HttpHost httpHost = new HttpHost(inference.getWebhookURI());
            this.restClient = RestClient.builder(httpHost).build();
        }


        public void nextStep(final InferenceExecutionMetadata metadata, final InferenceExecutionData data) {
            final UEBAJobExecutionMetadata.ExecutionState state = metadata.getState();
            try {
                switch (state) {
                    case QUERYING:
                        findHighPriorityEntities(metadata);
                        break;

                    case COMPUTING:
                        callInferenceHandler(metadata, data);
                        break;

                    case INDEXING:
                        upsertEntities(metadata, data);
                        break;

                    case QUERYING_FAILURE:
                    case COMPUTING_FAILURE:
                    case INDEXING_FAILURE:
                    case FAILURE:
                        break;
                    case DONE:
                        listener.onResponse(new ExecuteInferenceResponse(metadata));
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + state);
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }

        }

        private void start(InferenceExecutionMetadata metadata) {
            nextStep(metadata, null);
        }

        private void findHighPriorityEntities(InferenceExecutionMetadata metadata) throws IOException {
            SearchRequest searchRequest = searchRequest(inference, metadata);

            client.search(searchRequest, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    System.out.println(searchResponse);
                    System.out.println(searchResponse.getHits());
                    System.out.println(searchResponse.getHits().getHits());
                    for(Object hit: searchResponse.getHits().getHits()) {
                        System.out.println(hit);
                    }
                    nextStep(metadataAfterQuerying(metadata, searchResponse), extractEntities(searchResponse));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        private InferenceExecutionMetadata metadataAfterQuerying(InferenceExecutionMetadata metadata, SearchResponse searchResponse){
            Object[] searchAfter = null;

            try {
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                searchAfter = searchHits[searchHits.length - 1].getSortValues();
            } catch (NullPointerException | ArrayIndexOutOfBoundsException e) {
                log.error("Query for scheduling inference " + inference.getId() + " returned zero results. ", e);
            }

            return new InferenceExecutionMetadata(inference, COMPUTING, searchAfter);
        }

        private SearchRequest searchRequest(EntityInference inference, InferenceExecutionMetadata metadata) throws IOException {
            XContentParser xcp = JsonXContent.jsonXContent.createParser(xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE, inference.getSearchRequestString());

            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(xcp).size(inference.getPageSize());

            if (metadata.hasSearchAfter())
                searchSourceBuilder.searchAfter(metadata.getSearchAfter());

            return new SearchRequest(inference.getSourceIndex()).source(searchSourceBuilder);
        }

        private void upsertEntities(InferenceExecutionMetadata metadata, InferenceExecutionData data){

            BulkRequest request = new BulkRequest();

            for (InferenceExecutionData.EntityDocument entityDocument: data.entities){
                UpdateRequest updateRequest = new UpdateRequest()
                        .index(inference.getEntityIndex())
                        .id(entityDocument.id)
                        .docAsUpsert(false)
                        .doc(entityDocument.fields);

                request.add(updateRequest);
            }

            if (request.numberOfActions() == 0){
                nextStep(metadataAfterUpserting(metadata), null);
                return;
            }

            request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            client.bulk(request, new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    nextStep(metadataAfterUpserting(metadata), null);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        private InferenceExecutionMetadata metadataAfterUpserting(InferenceExecutionMetadata metadata) {
            if(metadata.getSearchAfter() != null)
                return new InferenceExecutionMetadata(inference, QUERYING, metadata.getSearchAfter());

            return new InferenceExecutionMetadata(inference, DONE, null);
        }

        void callInferenceHandler(InferenceExecutionMetadata metadata, InferenceExecutionData data) throws IOException {
            Request request = new Request(RestRequest.Method.POST.name(), inference.getWebhookURI());
            request.setEntity(createHttpEntity(data));

            restClient.performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        InferenceExecutionData newData = parseInferenceResponse(response);

                        nextStep(metadataAfterInferenceCall(metadata), newData);

                    } catch (IOException e) {
                        this.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception exception) {

                }
            });
        }

        private InferenceExecutionMetadata metadataAfterInferenceCall(InferenceExecutionMetadata metadata) {
            return new InferenceExecutionMetadata(inference, INDEXING, metadata.getSearchAfter());
        }

        HttpEntity createHttpEntity(InferenceExecutionData data) throws IOException {
            return new StringEntity(toJsonString(data, inference), ContentType.APPLICATION_JSON);
        }

    }


    private String toJsonString(InferenceExecutionData data, EntityInference inference) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        builder.field(INFERENCE_ID, inference.getId());
        data.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        return IndexUtilsKt.string(builder);
    }

    public InferenceExecutionData parseInferenceResponse(Response response) throws IOException {
        XContentType xContentType = XContentType.fromMediaType(response.getEntity().getContentType().getValue());

        try (
                XContentParser xcp = xContentType.xContent()
                        .createParser(
                                xContentRegistry,
                                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                                response.getEntity().getContent()
                        )
        ) {
            InferenceExecutionData data = null;

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp);
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = xcp.currentName();
                xcp.nextToken();

                switch (fieldName) {
                    case InferenceExecutionData.BATCH_PAYLOAD_FIELD:
                        data = InferenceExecutionData.parse(xcp);
                        break;
                }
            }
            return data;
        }
    }

    private static InferenceExecutionData extractEntities(SearchResponse searchResponse) {
        Collection<InferenceExecutionData.EntityDocument> entitiesToSave = new ArrayList<>();

        for (var entry : searchResponse.getHits().getHits()) {
            String entityName = entry.getId();
            Map<String, Object> entityProperties = entry.getSourceAsMap();

            entitiesToSave.add(new InferenceExecutionData.EntityDocument(entityName, entityProperties));
        }

        return new InferenceExecutionData(entitiesToSave);
    }

    private static class InferenceExecutionData {
        public static final String BATCH_PAYLOAD_FIELD = "batch_payload";
        final Collection<EntityDocument> entities;

        InferenceExecutionData(Collection<EntityDocument> entities){
            this.entities = entities;
        }

        private static class EntityDocument implements ToXContent {
            public static final String ENTITY_ID_FIELD = "entity_id";
            public static final String PAYLOAD_FIELD = "entity_data";

            final String id;
            final Map<String, Object> fields;

            EntityDocument(String id, Map<String, Object> fields){
                this.id = id;
                this.fields = fields;
            }

            public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
                builder.startObject();
                builder.field(ENTITY_ID_FIELD, id);
                builder.field(PAYLOAD_FIELD, fields);

                return builder.endObject();
            }

            public static EntityDocument parse(XContentParser xcp) throws IOException {
                String id = null;
                Map<String, Object> fields = null;

                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp);
                while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                    String fieldName = xcp.currentName();
                    xcp.nextToken();

                    switch (fieldName) {
                        case ENTITY_ID_FIELD:
                            id = xcp.text();
                            break;
                        case PAYLOAD_FIELD:
                            fields = xcp.map();
                            break;
                    }
                }
                    return new EntityDocument(id, fields);
            }
        }

        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            return builder.array(BATCH_PAYLOAD_FIELD, entities);
        }

        public static InferenceExecutionData parse(XContentParser xcp) throws IOException {
            List<EntityDocument> list = xcp.list().stream()
                    .map(x->(EntityDocument)x )
                    .collect( Collectors.toList() );

            return new InferenceExecutionData(list);
        }
    }
}
