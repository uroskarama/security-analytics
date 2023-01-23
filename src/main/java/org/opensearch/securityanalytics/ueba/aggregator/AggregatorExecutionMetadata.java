package org.opensearch.securityanalytics.ueba.aggregator;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.Map;

public class AggregatorExecutionMetadata implements ToXContentObject, Writeable {

    public static final String NO_ID = "";
    public static final String AGGREGATOR_ID_FIELD = "aggregator_id";
    public static final String AGGREGATOR_STATE_FIELD = "aggregator_state_field";
    public static final String AFTER_KEY_FIELD = "after_key";

    private Long seqNo;
    private Long primaryTerm;
    private final String aggregatorId;
    private final AggregatorExecutionState state;
    private final Map<String, Object> afterKey;

    public enum AggregatorExecutionState {
        AGGREGATING,
        INDEXING,
        AGGREGATING_FAILURE,
        INDEXING_FAILURE,
        FAILURE,
        DONE
    }

    public AggregatorExecutionMetadata(String aggregatorId, AggregatorExecutionState state, Map<String, Object> afterKey) {
        this.aggregatorId = aggregatorId;
        this.state = state;
        this.afterKey = afterKey;
    }

    public AggregatorExecutionMetadata(UebaAggregator aggregator, AggregatorExecutionState state, Map<String, Object> afterKey) {
        this(aggregator.getId(), state, afterKey);
    }

    public AggregatorExecutionMetadata(UebaAggregator aggregator, AggregatorExecutionState state) {
        this(aggregator.getId(), state, null);
    }

    public AggregatorExecutionMetadata(StreamInput sin) throws IOException {
        this(
                sin.readString(),
                sin.readEnum(AggregatorExecutionState.class),
                sin.readMap()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(aggregatorId);
        out.writeEnum(state);
        out.writeMap(afterKey);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(AGGREGATOR_ID_FIELD, aggregatorId);
        builder.field(AGGREGATOR_STATE_FIELD, state);
        builder.field(AFTER_KEY_FIELD, afterKey);
        return builder.endObject();
    }

    public static AggregatorExecutionMetadata parse(XContentParser xcp, String id, Long seqNo, Long primaryTerm) throws IOException {
        if (id == null) {
            id = NO_ID;
        }

        String aggregatorId = null;
        AggregatorExecutionState state = null;
        Map<String, Object> afterKey = null;

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp);
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = xcp.currentName();
                xcp.nextToken();

                switch (fieldName) {
                    case AGGREGATOR_ID_FIELD:
                        aggregatorId = xcp.text();
                        break;
                    case AGGREGATOR_STATE_FIELD:
                        state = xcp.namedObject(AggregatorExecutionState.class, xcp.currentName(), null);
                        break;
                    case AFTER_KEY_FIELD:
                        afterKey = xcp.map();
                        break;
                }
            }

            if (aggregatorId == null)
                throw new IOException("Missing aggregatorId while parsing " + AggregatorExecutionMetadata.class);

            if (state == null)
                throw new IOException("Missing state while parsing " + AggregatorExecutionMetadata.class);

            return new AggregatorExecutionMetadata(aggregatorId, state, afterKey);
        }

    public Map<String, Object> getAfterKey() {
        return afterKey;
    }

    public AggregatorExecutionState getState() {
        return state;
    }

    public boolean hasAfterKey() {
        return afterKey != null;
    }
}
