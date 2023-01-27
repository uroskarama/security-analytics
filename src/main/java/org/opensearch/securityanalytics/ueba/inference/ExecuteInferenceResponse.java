package org.opensearch.securityanalytics.ueba.inference;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class ExecuteInferenceResponse extends ActionResponse implements ToXContentObject {

    final InferenceExecutionMetadata metadata;

    public ExecuteInferenceResponse(InferenceExecutionMetadata metadata){
        this.metadata = metadata;
    }

    public ExecuteInferenceResponse(StreamInput sin) throws IOException {
        this(new InferenceExecutionMetadata(sin));
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


    public InferenceExecutionMetadata getMetadata() {
        return metadata;
    }

}
