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

public class IndexUebaAggregatorResponse extends ActionResponse implements ToXContentObject {

    private String id;

    private Long version;

    private RestStatus status;

    private UebaAggregator uebaAggregator;

    public IndexUebaAggregatorResponse(String id, Long version, RestStatus status, UebaAggregator uebaAggregator) {
        super();
        this.id = id;
        this.version = version;
        this.status = status;
        this.uebaAggregator = uebaAggregator;
    }

    public IndexUebaAggregatorResponse(StreamInput sin) throws IOException {
        this(sin.readString(),
             sin.readLong(),
             sin.readEnum(RestStatus.class),
                UebaAggregator.readFrom(sin));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(version);
        out.writeEnum(status);
        uebaAggregator.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version);
        builder.startObject("uebaAggregator")
            .field(UebaAggregator.SCHEDULE_FIELD, uebaAggregator.getSchedule())
            .field(UebaAggregator.LAST_UPDATE_TIME_FIELD, uebaAggregator.getLastUpdateTime())
            .field(UebaAggregator.ENABLED_TIME_FIELD, uebaAggregator.getEnabledTime())
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

    public UebaAggregator getUebaAggregator() {
        return uebaAggregator;
    }
}