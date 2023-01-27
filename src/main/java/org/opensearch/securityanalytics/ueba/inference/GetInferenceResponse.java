/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.inference;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;
import org.opensearch.securityanalytics.ueba.aggregator.UebaAggregator;

import java.io.IOException;

import static org.opensearch.securityanalytics.util.RestHandlerUtils._ID;
import static org.opensearch.securityanalytics.util.RestHandlerUtils._VERSION;

public class GetInferenceResponse extends ActionResponse implements ToXContentObject {

    private final String id;

    private final Long version;

    private final RestStatus status;

    private final EntityInference inference;

    public GetInferenceResponse(String id, Long version, RestStatus status, EntityInference inference) {
        super();
        this.id = id;
        this.version = version;
        this.status = status;
        this.inference = inference;
    }

    public GetInferenceResponse(StreamInput sin) throws IOException {
        this(sin.readString(),
             sin.readLong(),
             sin.readEnum(RestStatus.class),
             sin.readBoolean()? EntityInference.readFrom(sin): null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(version);
        out.writeEnum(status);
        if (inference != null) {
            out.writeBoolean(true);
            inference.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
                .field(_ID, id)
                .field(_VERSION, version);
        builder.startObject("aggregator")
                .field(UebaAggregator.ENABLED_FIELD, inference.getEnabled())
                .field(UebaAggregator.SCHEDULE_FIELD, inference.getSchedule())
                .field(UebaAggregator.LAST_UPDATE_TIME_FIELD, inference.getLastUpdateTime())
                .field(UebaAggregator.ENABLED_TIME_FIELD, inference.getEnabledTime())
                .field(UebaAggregator.SEARCH_REQUEST_STRING_FIELD, inference.getSearchRequestString())
                .field(UebaAggregator.SOURCE_INDEX_FIELD, inference.getSourceIndex())
                .field(UebaAggregator.BATCH_SIZE_FIELD, inference.getPageSize())
                .field(UebaAggregator.ENTITY_INDEX_FIELD, inference.getEntityIndex())
                .endObject();
        return builder.endObject();
    }

    public String getId() {
        return id;
    }

    public Long getVersion() {
        return version;
    }

    public RestStatus getStatus() {
        return status;
    }

    public EntityInference getInference() {
        return inference;
    }
}
