package org.opensearch.securityanalytics.ueba;

import org.apache.http.HttpStatus;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.rest.RestStatus;
import org.opensearch.securityanalytics.SecurityAnalyticsPlugin;
import org.opensearch.securityanalytics.SecurityAnalyticsRestTestCase;
import org.opensearch.securityanalytics.model.Detector;
import org.opensearch.securityanalytics.ueba.aggregator.UebaAggregator;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.opensearch.securityanalytics.TestHelpers.*;
import static org.opensearch.securityanalytics.TestHelpers.randomDetector;

public class AggregatorRestApiIT extends SecurityAnalyticsRestTestCase {

    public void testIndexAggregator() throws IOException {
        UebaAggregator aggregator = randomUebaAggregator();

        String jsonString = toJsonString(aggregator);
        System.out.println(jsonString);

        Response createResponse = makeRequest(client(),
                "POST",
                SecurityAnalyticsPlugin.UEBA_AGGREGATOR_BASE_URI,
                Collections.emptyMap(),
                toHttpEntity(aggregator)
                );

        assertEquals("Create aggregator failed", RestStatus.CREATED, restStatus(createResponse));
    }
}
