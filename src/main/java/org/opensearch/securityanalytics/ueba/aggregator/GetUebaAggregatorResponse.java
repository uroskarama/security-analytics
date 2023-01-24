/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.aggregator;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

import static org.opensearch.securityanalytics.util.RestHandlerUtils._ID;
import static org.opensearch.securityanalytics.util.RestHandlerUtils._VERSION;

public class GetUebaAggregatorResponse extends ActionResponse implements ToXContentObject {

    private final String id;

    private final Long version;

    private final RestStatus status;

    private final UebaAggregator aggregator;

    public GetUebaAggregatorResponse(String id, Long version, RestStatus status, UebaAggregator aggregator) {
        super();
        this.id = id;
        this.version = version;
        this.status = status;
        this.aggregator = aggregator;
    }

    public GetUebaAggregatorResponse(StreamInput sin) throws IOException {
        this(sin.readString(),
             sin.readLong(),
             sin.readEnum(RestStatus.class),
             sin.readBoolean()? UebaAggregator.readFrom(sin): null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(version);
        out.writeEnum(status);
        if (aggregator != null) {
            out.writeBoolean(true);
            aggregator.writeTo(out);
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
                .field(UebaAggregator.ENABLED_FIELD, aggregator.getEnabled())
                .field(UebaAggregator.SCHEDULE_FIELD, aggregator.getSchedule())
                .field(UebaAggregator.LAST_UPDATE_TIME_FIELD, aggregator.getLastUpdateTime())
                .field(UebaAggregator.ENABLED_TIME_FIELD, aggregator.getEnabledTime())
                .field(UebaAggregator.SEARCH_REQUEST_STRING_FIELD, aggregator.getSearchRequestString())
                .field(UebaAggregator.SOURCE_INDEX_FIELD, aggregator.getSourceIndex())
                .field(UebaAggregator.BATCH_SIZE_FIELD, aggregator.getPageSize())
                .field(UebaAggregator.ENTITY_INDEX_FIELD, aggregator.getEntityIndex())
                .field(UebaAggregator.ENTITY_FIELD_NAME_FIELD, aggregator.getEntityFieldName())
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

    public UebaAggregator getAggregator() {
        return aggregator;
    }
}
