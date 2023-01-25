package org.opensearch.securityanalytics.ueba.aggregator;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class ExecuteAggregatorResponse extends ActionResponse implements ToXContentObject {

    final AggregatorExecutionMetadata metadata;

    public ExecuteAggregatorResponse(AggregatorExecutionMetadata metadata){
        this.metadata = metadata;
    }

    public ExecuteAggregatorResponse(StreamInput sin) throws IOException {
        this(new AggregatorExecutionMetadata(sin));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        metadata.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        xContentBuilder.startObject();
        xContentBuilder.field("metadata");
        metadata.toXContent(xContentBuilder, params);
        return xContentBuilder.endObject();
    }


    public AggregatorExecutionMetadata getMetadata() {
        return metadata;
    }

}
