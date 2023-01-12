package org.opensearch.securityanalytics.ueba.aggregator;

import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public class UebaAggregatorMetadata implements ToXContentObject {

    private Map<String, Object> afterKey = null;


    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        return null;
    }

    public static UebaAggregatorMetadata parse(XContentParser parser) {
        return null; // TODO
    }
}
