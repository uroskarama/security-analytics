package org.opensearch.securityanalytics.ueba.aggregator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

public class AggregatorService {

    private static final Logger log = LogManager.getLogger(AggregatorService.class);

    private final Client client;

    public AggregatorService(Client client) {
        this.client = client;
    }



    public void execute(UebaAggregator aggregator, ActionListener<SearchResponse> listener){
        String searchRequestString = aggregator.getSearchRequestString();
        String sourceIndex = aggregator.getSourceIndex();
        Long batchSize = aggregator.getBatchSize();

        try {
            SearchRequest searchRequest = parseSearchRequestString(searchRequestString, sourceIndex, batchSize, null);

            client.search(searchRequest, listener);


        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    private class ContinuationListener implements ActionListener<SearchResponse> {

        @Override
        public void onResponse(SearchResponse searchResponse) {
            searchResponse.getAggregations();
        }

        @Override
        public void onFailure(Exception e) {

        }
    }

    private SearchRequest parseSearchRequestString(String searchRequestString, String index, Long size, java.util.Map<String, Object> afterKey) throws IOException {
        XContentParser xcp = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, searchRequestString
        );

        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(xcp);
        searchSourceBuilder = searchSourceBuilder.size(0);

        if (size > 0 && afterKey != null){
            for (AggregationBuilder aggregationBuilder: searchSourceBuilder.aggregations().getAggregatorFactories()){
                if (aggregationBuilder instanceof CompositeAggregationBuilder){
                    CompositeAggregationBuilder compositeAggregationBuilder = (CompositeAggregationBuilder)aggregationBuilder;
                    if (size > 0)
                        compositeAggregationBuilder.size(Math.toIntExact(size));

                    if (afterKey != null)
                        compositeAggregationBuilder.aggregateAfter(afterKey);
                }
            }
        }

        return new SearchRequest(index)
                .source(searchSourceBuilder)
                .allowPartialSearchResults(false);
    }
}
