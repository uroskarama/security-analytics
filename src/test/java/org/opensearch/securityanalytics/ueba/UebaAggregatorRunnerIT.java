package org.opensearch.securityanalytics.ueba;

import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.commons.alerting.util.IndexUtilsKt;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.search.SearchHit;
import org.opensearch.securityanalytics.SecurityAnalyticsRestTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class UebaAggregatorRunnerIT extends SecurityAnalyticsRestTestCase {

    public void testRunUebaJob_success() throws IOException {

        // TODO: Create UEBA Job
        String index = ".opendistro-sa-config";

        String testId = "testId";
        UebaAggregator uebaAggregatorJob = new UebaAggregator(testId, true, Instant.now(), Instant.now(), new IntervalSchedule(Instant.now(), 5,  ChronoUnit.MINUTES), 5L, 5L);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        String uebaJobJson = IndexUtilsKt.string(shuffleXContent(uebaAggregatorJob.toXContent(builder, ToXContent.EMPTY_PARAMS)));
        Request createUebaJobRequest = new Request("POST", index + "/_doc");
        createUebaJobRequest.setJsonEntity(
                uebaJobJson
        );
        Response response = client().performRequest(createUebaJobRequest);
        assertEquals(HttpStatus.SC_CREATED, response.getStatusLine().getStatusCode());

        String request = "{\n" +
                "   \"query\" : {\n" +
                "     \"match\":{\n" +
                "        \"id\": \"" + testId + "\"\n" +
                "     }\n" +
                "   }\n" +
                "}";
        List<SearchHit> hits = executeSearch(index, request);
        SearchHit hit = hits.get(0);

        Assert.assertNotNull(hit);
    }
}
