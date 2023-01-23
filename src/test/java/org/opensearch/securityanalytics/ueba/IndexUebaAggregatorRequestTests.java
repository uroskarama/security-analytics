package org.opensearch.securityanalytics.ueba;

import org.junit.Assert;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.rest.RestRequest;
import org.opensearch.securityanalytics.ueba.aggregator.IndexUebaAggregatorRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.UUID;

import static org.opensearch.securityanalytics.TestHelpers.randomUebaAggregator;

public class IndexUebaAggregatorRequestTests extends OpenSearchTestCase {

    public void testIndexUebaAggregatorPostRequest() throws IOException {
        String aggregatorId = UUID.randomUUID().toString();
        IndexUebaAggregatorRequest request = new IndexUebaAggregatorRequest(aggregatorId, WriteRequest.RefreshPolicy.IMMEDIATE, RestRequest.Method.POST, randomUebaAggregator());

        Assert.assertNotNull(request);

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);



        StreamInput sin = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        IndexUebaAggregatorRequest newRequest = new IndexUebaAggregatorRequest(sin);
        Assert.assertEquals(aggregatorId, request.getAggregatorId());
        Assert.assertEquals(RestRequest.Method.POST, newRequest.getMethod());
        Assert.assertNotNull(newRequest.getUebaAggregator());
    }
}

