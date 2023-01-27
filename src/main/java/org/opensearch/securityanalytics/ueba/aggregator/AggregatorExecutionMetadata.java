package org.opensearch.securityanalytics.ueba.aggregator;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.securityanalytics.ueba.core.UEBAJobExecutionMetadata;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class AggregatorExecutionMetadata extends UEBAJobExecutionMetadata {

    public static final String AGGREGATOR_TYPE = UebaAggregator.AGGREGATOR_TYPE;
    public static final String AFTER_KEY_FIELD = "after_key";
    private final Map<String, Object> afterKey;

    public AggregatorExecutionMetadata(String id, ExecutionState state, Map<String, Object> afterKey) {
        super(id, AGGREGATOR_TYPE, state);
        this.afterKey = afterKey;
    }

    public AggregatorExecutionMetadata(UebaAggregator aggregator, ExecutionState state, Map<String, Object> afterKey) {
        super(aggregator.getId(), aggregator.getType(), state);
        this.afterKey = afterKey;
    }

    public AggregatorExecutionMetadata(UebaAggregator aggregator, ExecutionState state) {
        super(aggregator, state);
        this.afterKey = null;
    }

    public AggregatorExecutionMetadata(StreamInput sin) throws IOException {
        super(sin);
        this.afterKey = sin.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(afterKey);
    }

    public static UEBAJobExecutionMetadata parse(XContentParser xcp, String id, Long seqNo, Long primaryTerm) throws IOException {
        if (id == null) {
            id = NO_ID;
        }

        String aggregatorId = null;
        String type = null;
        ExecutionState state = null;
        Map<String, Object> afterKey = null;

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp);
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = xcp.currentName();
            xcp.nextToken();

            switch (fieldName) {
                case JOB_ID_FIELD:
                    aggregatorId = xcp.text();
                    break;
                case JOB_TYPE_FIELD:
                    type = xcp.text();
                    assert Objects.equals(type, AGGREGATOR_TYPE);
                    break;
                case JOB_STATE_FIELD:
                    state = xcp.namedObject(ExecutionState.class, xcp.currentName(), null);
                    break;
                case AFTER_KEY_FIELD:
                    afterKey = xcp.map();
                    break;
            }
        }

        if (aggregatorId == null)
            throw new IOException("Missing aggregatorId while parsing " + UEBAJobExecutionMetadata.class);

        if (state == null)
            throw new IOException("Missing state while parsing " + UEBAJobExecutionMetadata.class);

        return new AggregatorExecutionMetadata(aggregatorId, state, afterKey);
    }

    public Map<String, Object> getAfterKey() {
        return afterKey;
    }

    public boolean hasAfterKey() {
        return afterKey != null;
    }

    @Override
    public String toString() {
        return "AggregatorExecutionMetadata{" +
                "seqNo=" + seqNo +
                ", primaryTerm=" + primaryTerm +
                ", aggregatorId='" + id + '\'' +
                ", state=" + state +
                ", afterKey=" + afterKey +
                '}';
    }
}
