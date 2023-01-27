package org.opensearch.securityanalytics.ueba;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.alerting.util.IndexUtilsKt;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.rest.RestStatus;
import org.opensearch.securityanalytics.SecurityAnalyticsPlugin;
import org.opensearch.securityanalytics.SecurityAnalyticsRestTestCase;
import org.opensearch.securityanalytics.ueba.aggregator.UebaAggregator;
import org.opensearch.securityanalytics.ueba.inference.EntityInference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AggregatorServiceIT extends SecurityAnalyticsRestTestCase {
    private static final Logger log = LogManager.getLogger(AggregatorServiceIT.class);

    public static final String AUTH_SAMPLE_DATA = "ueba/auth.log_login_events_sample.json";
    public static final String WINLOG_SAMPLE_DATA = "ueba/winlog_login_events_sample.json";
    public static final String CLOUDTRAIL_SAMPLE_DATA = "ueba/cloudtrail_login_events_sample.json";

    public static final String AUTH_ITT_AGGREGATION_QUERY = "aggregations/impossible_time_travel/auth.json";
    public static final String WINLOG_ITT_AGGREGATION_QUERY = "aggregations/impossible_time_travel/winlog.json";
    public static final String CLOUDTRAIL_ITT_AGGREGATION_QUERY = "aggregations/impossible_time_travel/cloudtrail.json";

    public static final String WINLOG_TEST_INDEX = "winlog_test_index";
    public static final String AUTH_TEST_INDEX = "auth_test_index";
    public static final String CLOUDTRAIL_TEST_INDEX = "cloudtrail_test_index";


    public static final String ENTITY_INDEX = "entity_index";

    private enum TestSample {

        WINLOG(WINLOG_SAMPLE_DATA, WINLOG_TEST_INDEX, WINLOG_ITT_AGGREGATION_QUERY),
        AUTH(AUTH_SAMPLE_DATA, AUTH_TEST_INDEX, AUTH_ITT_AGGREGATION_QUERY),
        CLOUDTRAIL(CLOUDTRAIL_SAMPLE_DATA, CLOUDTRAIL_TEST_INDEX, CLOUDTRAIL_ITT_AGGREGATION_QUERY);

        final String datasetPath;
        final String indexName;
        final String queryPath;

        TestSample(String datasetPath, String indexName, String queryPath){
            this.datasetPath = datasetPath;
            this.indexName = indexName;
            this.queryPath = queryPath;
        }
    }

    private void createEntityIndex() throws IOException {
        Response createResponse = makeRequest(client(),
                "PUT",
                ENTITY_INDEX,
                Collections.emptyMap(),
                null
        );

        assertEquals("Create entity index failed", RestStatus.OK, restStatus(createResponse));
    }

    private void indexSampleData(TestSample testSample) throws IOException {
        String data = readResource(testSample.datasetPath);

        XContentParser xcp = XContentType.JSON.xContent().createParser(
                this.xContentRegistry(),
                LoggingDeprecationHandler.INSTANCE, data
        );

        List<Object> documents = xcp.list();

        StringBuilder sb = new StringBuilder();

        for(Object document:documents){
            if (!(document instanceof Map)){
                throw new RuntimeException("Expected Map<String,?> got "
                        + document.getClass() + ", is sample dataset broken?");
            }

            String action = "{ \"create\": { \"_index\":\"" + testSample.indexName + "\" }}\n";

            Map<String, ?> docToIndex = (Map<String, ?>)document;
            String doc = IndexUtilsKt.string(XContentFactory.jsonBuilder().map(docToIndex));

            sb.append(action)
                    .append(doc)
                    .append("\n");
        }

        Response bulkResponse = makeRequest(client(),
                "POST",
                ENTITY_INDEX + "/_bulk",
                Collections.emptyMap(),
                new StringEntity(sb.toString(), ContentType.APPLICATION_JSON)
        );

        assertEquals("Failure indexing sample dataset: " + testSample.name(), RestStatus.OK, restStatus(bulkResponse));

        Map<String, Object> responseMap = asMap(bulkResponse);

        List<Map<String, Object>> responseItems = (List)responseMap.get("items");
        for (Map<String, Object> item: responseItems) {
            assertEquals("Indexing item failed", RestStatus.CREATED.getStatus(), ((Map<String, ?>)item.get("create")).get("status"));
        }
    }

    private UebaAggregator createAggregator(TestSample testSample) throws IOException {
        String searchRequestString = readResource(testSample.queryPath);

        return new UebaAggregator("aggregator1234",
                true,
                Instant.now(),
                Instant.now(),
                new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                1L,
                1L,
                searchRequestString,
                testSample.indexName,
                10,
                ENTITY_INDEX,
                "");
    }

    private void indexAggregator(UebaAggregator aggregator) throws IOException {
        Response createResponse = makeRequest(client(),
                "PUT",
                SecurityAnalyticsPlugin.UEBA_AGGREGATOR_BASE_URI + "/" + aggregator.getId(),
                Collections.emptyMap(),
                toHttpEntity(aggregator)
        );

        assertEquals("Create aggregator failed", RestStatus.OK, restStatus(createResponse));
    }

    private void executeAggregator(UebaAggregator aggregator) throws IOException {
        log.debug("Executing aggregator: " + aggregator);

        Response executeResponse = makeRequest(client(),
                "POST",
                SecurityAnalyticsPlugin.UEBA_AGGREGATOR_BASE_URI + "/" + aggregator.getId() + "/_execute",
                Collections.emptyMap(),
                new StringEntity("{}", ContentType.APPLICATION_JSON)
        );

        log.debug("Execute response: " + new String(executeResponse.getEntity().getContent().readAllBytes()));

        assertEquals("Execute aggregator failed", RestStatus.OK, restStatus(executeResponse));
    }

    private void executeInference(EntityInference inference) throws IOException {
        log.debug("Executing inference: " + inference);

        Response executeResponse = makeRequest(client(),
                "POST",
                SecurityAnalyticsPlugin.UEBA_INFERENCE_BASE_URI + "/" + inference.getId() + "/_execute",
                Collections.emptyMap(),
                new StringEntity("{}", ContentType.APPLICATION_JSON)
        );

        log.debug("Execute response: " + new String(executeResponse.getEntity().getContent().readAllBytes()));

        assertEquals("Execute inference failed", RestStatus.OK, restStatus(executeResponse));
    }

    private void searchEntities() throws IOException {
        Response searchResponse = makeRequest(client(),
                "GET",
                ENTITY_INDEX + "/_search?pretty",
                Collections.emptyMap(),
                new StringEntity("{ \"query\": { \"match_all\": {} } }", ContentType.APPLICATION_JSON),
                new BasicHeader("Content-Type", "application/json")
        );

        log.debug("Entities found: " + new String(searchResponse.getEntity().getContent().readAllBytes()));
        System.out.println("Entities found: " + new String(searchResponse.getEntity().getContent().readAllBytes()));
    }

    private EntityInference createInference() throws IOException {
        String searchRequestString = "{ \"query\": { \"match_all\": {} } }";

        return new EntityInference("inference1234",
                true,
                Instant.now(),
                Instant.now(),
                new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                1L,
                1L,
                searchRequestString,
                ENTITY_INDEX,
                10,
                ENTITY_INDEX,
                "http://localhost:8080/itt");
    }

    private void indexInference(EntityInference inference) throws IOException {
        Response createResponse = makeRequest(client(),
                "PUT",
                SecurityAnalyticsPlugin.UEBA_INFERENCE_BASE_URI + "/" + inference.getId(),
                Collections.emptyMap(),
                toHttpEntity(inference)
        );

        assertEquals("Create aggregator failed", RestStatus.OK, restStatus(createResponse));

    }

    public void runAggregatorServiceTest(TestSample testSample) throws IOException, InterruptedException {
        createEntityIndex();

        indexSampleData(testSample);

        UebaAggregator aggregator = createAggregator(testSample);

        indexAggregator(aggregator);

        executeAggregator(aggregator);

        searchEntities();

        EntityInference inference = createInference();

        indexInference(inference);

        //executeInference(inference);

        Thread.sleep(1000000);
    }

    public void testAggregatorServiceOnWinlogData() throws IOException, InterruptedException {
        runAggregatorServiceTest(TestSample.WINLOG);
    }

    public void testAggregatorServiceOnAuthData() throws IOException, InterruptedException {
        runAggregatorServiceTest(TestSample.AUTH);
    }

    public void testAggregatorServiceOnCloudtrailData() throws IOException, InterruptedException {
        runAggregatorServiceTest(TestSample.CLOUDTRAIL);
    }

    private String readResource(String name) throws IOException {
        try (InputStream inputStream = SecurityAnalyticsPlugin.class.getClassLoader().getResourceAsStream(name)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + name);
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                return reader.lines().collect(Collectors.joining("\n"));
            }
        }
    }
}
