package org.opensearch.securityanalytics.ueba;

import org.junit.Assert;
import org.opensearch.client.Response;
import org.opensearch.rest.RestStatus;
import org.opensearch.securityanalytics.SecurityAnalyticsPlugin;
import org.opensearch.securityanalytics.SecurityAnalyticsRestTestCase;
import org.opensearch.securityanalytics.ueba.aggregator.UebaAggregator;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.securityanalytics.TestHelpers.randomUebaAggregator;

public class AggregatorRestApiIT extends SecurityAnalyticsRestTestCase {

    public void testIndexAggregator() throws IOException {
        UebaAggregator aggregator = randomUebaAggregator();

        Response createResponse = makeRequest(client(),
                "POST",
                SecurityAnalyticsPlugin.UEBA_AGGREGATOR_BASE_URI,
                Map.of("aggregator_id", aggregator.getId()),
                toHttpEntity(aggregator)
                );

        assertEquals("Create aggregator failed", RestStatus.CREATED, restStatus(createResponse));

        Response getResponse = makeRequest(client(),
                "GET",
                String.format(Locale.getDefault(), "%s/%s", SecurityAnalyticsPlugin.UEBA_AGGREGATOR_BASE_URI, aggregator.getId()),
                Collections.emptyMap(),
                null);

        assertEquals("Get aggregator failed", RestStatus.OK, restStatus(getResponse));

        Map<String, Object> responseBody = asMap(createResponse);

        String createdId = responseBody.get("_id").toString();

        Assert.assertNotEquals("response is missing Id", UebaAggregator.NO_ID, createdId);
        Assert.assertEquals("Incorrect Location header", String.format(Locale.getDefault(), "%s/%s", SecurityAnalyticsPlugin.UEBA_AGGREGATOR_BASE_URI, createdId), createResponse.getHeader("Location"));
        Assert.assertFalse(responseBody.containsKey("search_request_string"));
        Assert.assertFalse(responseBody.containsKey("entity_index"));
        Assert.assertFalse(responseBody.containsKey("source_index"));
    }
}
