package org.opensearch.securityanalytics.ueba.aggregator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ScriptedMetric;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

import static org.opensearch.securityanalytics.ueba.core.UEBAJobExecutionMetadata.ExecutionState.*;

public class AggregatorService {

    private static final Logger log = LogManager.getLogger(AggregatorService.class);

    private final Client client;

    private final NamedXContentRegistry xContentRegistry;

    public AggregatorService(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    public void execute(UebaAggregator aggregator, ActionListener<ExecuteAggregatorResponse> listener){
        AggregatorExecutionMetadata metadata = new AggregatorExecutionMetadata(aggregator, AGGREGATING);

        EventAggregatorFSM eventAggregatorFSM = new EventAggregatorFSM(aggregator, listener);

        eventAggregatorFSM.start(metadata);
    }


    private class EventAggregatorFSM {

        private final ActionListener<ExecuteAggregatorResponse> listener;

        private final UebaAggregator aggregator;

        EventAggregatorFSM(UebaAggregator aggregator, ActionListener<ExecuteAggregatorResponse> listener){
            this.aggregator = aggregator;
            this.listener = listener;
        }

        public void nextStep(final AggregatorExecutionMetadata metadata, final AggregatorExecutionData data) {
            final AggregatorExecutionMetadata.ExecutionState state = metadata.getState();
            try {
                switch (state){
                    case AGGREGATING:
                        aggregateEvents(metadata);
                        break;

                    case INDEXING:
                        upsertEntities(metadata, data);
                        break;
                    case AGGREGATING_FAILURE:
                    case INDEXING_FAILURE:
                    case FAILURE:
                        break;
                    case DONE:
                        listener.onResponse(new ExecuteAggregatorResponse(metadata));
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + state);
                }
            } catch (IOException e) {
                listener.onFailure(e);
            }

        }

        private void start(AggregatorExecutionMetadata metadata){
            nextStep(metadata, null);
        }

        private void aggregateEvents(AggregatorExecutionMetadata metadata) throws IOException {
            SearchRequest searchRequest = searchRequest(aggregator, metadata);

            client.search(searchRequest, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    nextStep(metadataAfterAggregating(metadata, searchResponse), extractEntities(searchResponse));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        private AggregatorExecutionMetadata metadataAfterAggregating(AggregatorExecutionMetadata metadata, SearchResponse searchResponse){
            Map<String, Object> afterKey = null;

            for(Aggregation aggregation: searchResponse.getAggregations()){
                if (aggregation instanceof CompositeAggregation)
                    afterKey = ((CompositeAggregation) aggregation).afterKey();
            }

            return new AggregatorExecutionMetadata(aggregator, INDEXING, afterKey);
        }

        private void upsertEntities(AggregatorExecutionMetadata metadata, AggregatorExecutionData data){

            BulkRequest request = new BulkRequest();

            for (AggregatorExecutionData.EntityDocument entityDocument: data.entities){
                UpdateRequest updateRequest = new UpdateRequest()
                        .index(aggregator.getEntityIndex())
                        .id(entityDocument.id)
                        .docAsUpsert(true)
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
                public void onResponse(BulkResponse bulkResponse) {
                    nextStep(metadataAfterUpserting(metadata), null);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        private AggregatorExecutionMetadata metadataAfterUpserting(AggregatorExecutionMetadata metadata) {
            if(metadata.getAfterKey() != null)
                return new AggregatorExecutionMetadata(aggregator, AGGREGATING, metadata.getAfterKey());

            return new AggregatorExecutionMetadata(aggregator, DONE, metadata.getAfterKey());
        }
    }

    private SearchRequest parseSearchRequestString(String searchRequestString, String index) throws IOException {
        XContentParser xcp = JsonXContent.jsonXContent.createParser(xContentRegistry,
                LoggingDeprecationHandler.INSTANCE, searchRequestString);

        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(xcp).size(0);

        return new SearchRequest(index)
                .source(searchSourceBuilder)
                .allowPartialSearchResults(false);
    }

    private SearchRequest searchRequest(UebaAggregator aggregator, AggregatorExecutionMetadata metadata) throws IOException {
        SearchRequest searchRequest = parseSearchRequestString(aggregator.getSearchRequestString(), aggregator.getSourceIndex());

        SearchSourceBuilder searchSourceBuilder = searchRequest.source();

        for (AggregationBuilder aggregationBuilder: searchSourceBuilder.aggregations().getAggregatorFactories()){
            if (aggregationBuilder instanceof CompositeAggregationBuilder){
                CompositeAggregationBuilder compositeAggregationBuilder = (CompositeAggregationBuilder)aggregationBuilder;

                compositeAggregationBuilder.size(Math.toIntExact(aggregator.getPageSize()));

                if (metadata.hasAfterKey())
                    compositeAggregationBuilder.aggregateAfter(metadata.getAfterKey());
            }
        }

        return searchRequest;
    }

    private static AggregatorExecutionData extractEntities(SearchResponse searchResponse) {
        Collection<AggregatorExecutionData.EntityDocument> entitiesToSave = new ArrayList<>();

        for (var entry : searchResponse.getAggregations().asMap().entrySet()) {
            String aggregationName = entry.getKey();
            Aggregation aggregation = entry.getValue();

            if (!(aggregation instanceof CompositeAggregation)) {
                continue; // Currently we support only CompositeAggregation.
            }

            CompositeAggregation bucketedAggregation = (CompositeAggregation) aggregation;

            for (CompositeAggregation.Bucket bucket : bucketedAggregation.getBuckets()) {
                String entityName = bucket.getKeyAsString();
                Map<String, Object> entityProperties = new LinkedHashMap<>();
                for (Aggregation agg : bucket.getAggregations()) {
                    if (agg instanceof ScriptedMetric) {  // Currently we support only ScriptedMetric aggregations.
                        ScriptedMetric scriptedMetricAgg = (ScriptedMetric) agg;

                        // String aggregatorName = aggregatorResponse.getMetadata().getName(); FIXME should we prepend aggregatorName to propertyName?

                        String propertyName = aggregationName + '.' + scriptedMetricAgg.getName();

                        entityProperties.put(propertyName, scriptedMetricAgg.aggregation());
                    }
                }

                entitiesToSave.add(new AggregatorExecutionData.EntityDocument(entityName, entityProperties));
            }
        }

        return new AggregatorExecutionData(entitiesToSave);
    }

    private static class AggregatorExecutionData {
        final Collection<EntityDocument> entities;

        AggregatorExecutionData(Collection<EntityDocument> entities){
            this.entities = entities;
        }

        private static class EntityDocument {

            final String id;
            final Map<String, Object> fields;

            EntityDocument(String id, Map<String, Object> fields){
                this.id = id;
                this.fields = fields;
            }
        }
    }
}
