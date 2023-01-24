/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.aggregator;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.rest.RestRequest;

import java.io.IOException;

public class IndexUebaAggregatorRequest extends ActionRequest {

    private final String aggregatorId;

    private final WriteRequest.RefreshPolicy refreshPolicy;

    private final RestRequest.Method method;

    private final UebaAggregator uebaAggregator;

    public IndexUebaAggregatorRequest(
            String aggregatorId,
            WriteRequest.RefreshPolicy refreshPolicy,
            RestRequest.Method method,
            UebaAggregator uebaAggregator) {
        super();
        this.aggregatorId = aggregatorId;
        this.refreshPolicy = refreshPolicy;
        this.method = method;
        this.uebaAggregator = uebaAggregator;
    }

    public IndexUebaAggregatorRequest(StreamInput sin) throws IOException {
        this(sin.readString(),
             WriteRequest.RefreshPolicy.readFrom(sin),
             sin.readEnum(RestRequest.Method.class),
                UebaAggregator.readFrom(sin));
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (uebaAggregator == null)
            validationException = ValidateActions.addValidationError("no aggregator specified", validationException);

        if (uebaAggregator.getId() == null || uebaAggregator.getId().length() == 0)
            validationException = ValidateActions.addValidationError("missing aggregator id", validationException);

        if (uebaAggregator.getSearchRequestString() == null || uebaAggregator.getSearchRequestString().length() == 0)
            validationException = ValidateActions.addValidationError("no search request string for aggregator specified", validationException);


        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(aggregatorId);
        refreshPolicy.writeTo(out);
        out.writeEnum(method);
        uebaAggregator.writeTo(out);
    }

    public String getAggregatorId() {
        return aggregatorId;
    }

    public RestRequest.Method getMethod() {
        return method;
    }

    public UebaAggregator getUebaAggregator() {
        return uebaAggregator;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }
}