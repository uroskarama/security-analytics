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

public class IndexEntityInferenceResponse extends ActionResponse implements ToXContentObject {

    private final String id;

    private final Long version;

    private final RestStatus status;

    private final EntityInference entityInference;

    public IndexEntityInferenceResponse(String id, Long version, RestStatus status, EntityInference entityInference) {
        super();
        this.id = id;
        this.version = version;
        this.status = status;
        this.entityInference = entityInference;
    }

    public IndexEntityInferenceResponse(StreamInput sin) throws IOException {
        this(sin.readString(),
             sin.readLong(),
             sin.readEnum(RestStatus.class),
                EntityInference.readFrom(sin));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(version);
        out.writeEnum(status);
        entityInference.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version);
        builder.startObject("inference")
            .field(UebaAggregator.SCHEDULE_FIELD, entityInference.getSchedule())
            .field(UebaAggregator.LAST_UPDATE_TIME_FIELD, entityInference.getLastUpdateTime())
            .field(UebaAggregator.ENABLED_TIME_FIELD, entityInference.getEnabledTime())
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

    public EntityInference getEntityInference() {
        return entityInference;
    }
}