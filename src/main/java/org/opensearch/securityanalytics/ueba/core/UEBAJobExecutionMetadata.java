package org.opensearch.securityanalytics.ueba.core;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class UEBAJobExecutionMetadata implements ToXContentObject, Writeable {

    public static final String NO_ID = "";
    public static final String JOB_ID_FIELD = "id";
    public static final String JOB_STATE_FIELD = "job_state_field";
    public static final String JOB_TYPE_FIELD = "type";


    protected Long seqNo;
    protected Long primaryTerm;
    protected final String id;
    protected final String type;
    protected final ExecutionState state;


    public enum ExecutionState {
        QUERYING,
        AGGREGATING,
        COMPUTING,
        INDEXING,
        QUERYING_FAILURE,
        AGGREGATING_FAILURE,
        COMPUTING_FAILURE,
        INDEXING_FAILURE,
        FAILURE,
        DONE
    }

    protected UEBAJobExecutionMetadata(String id, String type, ExecutionState state) {
        this.id = id;
        this.type = type;
        this.state = state;
    }

    protected UEBAJobExecutionMetadata(UEBAJobParameter parameter, ExecutionState state) {
        this(parameter.getId(), parameter.getType(), state);
    }

    public UEBAJobExecutionMetadata(StreamInput sin) throws IOException {
        this(
                sin.readString(),
                sin.readString(),
                sin.readEnum(ExecutionState.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(type);
        out.writeEnum(state);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(JOB_ID_FIELD, id);
        builder.field(JOB_TYPE_FIELD, type);
        builder.field(JOB_STATE_FIELD, state);
        return builder.endObject();
    }

    public String getJobType(){ return type; }

    public ExecutionState getState() {
        return state;
    }
}
