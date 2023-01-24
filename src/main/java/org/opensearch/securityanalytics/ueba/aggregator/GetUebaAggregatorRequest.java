/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.aggregator;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Locale;

import static org.opensearch.action.ValidateActions.addValidationError;

public class GetUebaAggregatorRequest extends ActionRequest {

    private final String aggregatorId;
    private final Long version;
    public static final String AGGREGATOR_ID = "aggregator_id";

    public GetUebaAggregatorRequest(String aggregatorId, Long version) {
        super();
        this.aggregatorId = aggregatorId;
        this.version = version;
    }
    public GetUebaAggregatorRequest(StreamInput sin) throws IOException {
        this(sin.readString(),
             sin.readLong());
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (aggregatorId == null || aggregatorId.length() == 0) {
            validationException = addValidationError(String.format(Locale.getDefault(), "%s is missing", AGGREGATOR_ID), validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(aggregatorId);
        out.writeLong(version);
    }

    public String getAggregatorId() {
        return aggregatorId;
    }

    public Long getVersion() {
        return version;
    }
}