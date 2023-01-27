package org.opensearch.securityanalytics.ueba.inference;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.securityanalytics.ueba.core.UEBAJobExecutionMetadata;

import java.io.IOException;

public class InferenceExecutionMetadata extends UEBAJobExecutionMetadata {

    public static final String INFERENCE_TYPE = EntityInference.INFERENCE_TYPE;
    public static final String SEARCH_AFTER_TYPE = "search_after";
    private final Object[] searchAfter;

    public InferenceExecutionMetadata(String id, ExecutionState state, Object[] search_after) {
        super(id, INFERENCE_TYPE, state);
        this.searchAfter = search_after;
    }

    public InferenceExecutionMetadata(EntityInference entityInference, ExecutionState state, Object[] searchAfter) {
        super(entityInference.getId(), entityInference.getType(), state);
        this.searchAfter = searchAfter;
    }

    public InferenceExecutionMetadata(EntityInference inference, ExecutionState state) {
        super(inference, state);
        this.searchAfter = null;
    }

    public InferenceExecutionMetadata(StreamInput sin) throws IOException {
        super(sin);
        int size = sin.readInt();
        this.searchAfter = new Object[size];
        for (int i=0; i<size; i++)
            this.searchAfter[i] = sin.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(searchAfter.length);
        for (Object value: searchAfter){
            out.writeGenericValue(value);
        }
    }

    public static UEBAJobExecutionMetadata parse(XContentParser xcp, String id, Long seqNo, Long primaryTerm) throws IOException {
        if (id == null) {
            id = NO_ID;
        }

        String jobId = null;
        String type = null;
        ExecutionState state = null;
        Object[] searchAfter = null;

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp);
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = xcp.currentName();
            xcp.nextToken();

            switch (fieldName) {
                case JOB_ID_FIELD:
                    jobId = xcp.text();
                    break;
                case JOB_TYPE_FIELD:
                    type = xcp.text();
                    assert type == INFERENCE_TYPE;
                    break;
                case JOB_STATE_FIELD:
                    state = xcp.namedObject(ExecutionState.class, xcp.currentName(), null);
                    break;
                case SEARCH_AFTER_TYPE:
                    searchAfter = xcp.list().toArray();
                    break;
            }
        }

        if (jobId == null)
            throw new IOException("Missing job id while parsing " + UEBAJobExecutionMetadata.class);

        if (state == null)
            throw new IOException("Missing state while parsing " + UEBAJobExecutionMetadata.class);

        return new InferenceExecutionMetadata(jobId, state, searchAfter);
    }

    public Object[] getSearchAfter() {
        return searchAfter;
    }

    public boolean hasSearchAfter() {
        return searchAfter != null;
    }

    @Override
    public String toString() {
        return "AggregatorExecutionMetadata{" +
                "seqNo=" + seqNo +
                ", primaryTerm=" + primaryTerm +
                ", aggregatorId='" + id + '\'' +
                ", state=" + state +
                ", searchAfter=" + searchAfter +
                '}';
    }
}
