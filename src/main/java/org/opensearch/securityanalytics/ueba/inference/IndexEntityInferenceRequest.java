/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.inference;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.rest.RestRequest;

import java.io.IOException;

public class IndexEntityInferenceRequest extends ActionRequest {

    private final String inferenceId;

    private final WriteRequest.RefreshPolicy refreshPolicy;

    private final RestRequest.Method method;

    private final EntityInference entityInference;

    public IndexEntityInferenceRequest(
            String inferenceId,
            WriteRequest.RefreshPolicy refreshPolicy,
            RestRequest.Method method,
            EntityInference entityInference) {
        super();
        this.inferenceId = inferenceId;
        this.refreshPolicy = refreshPolicy;
        this.method = method;
        this.entityInference = entityInference;
    }

    public IndexEntityInferenceRequest(StreamInput sin) throws IOException {
        this(sin.readString(),
             WriteRequest.RefreshPolicy.readFrom(sin),
             sin.readEnum(RestRequest.Method.class),
                EntityInference.readFrom(sin));
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (entityInference == null)
            validationException = ValidateActions.addValidationError("no inference specified", validationException);

        if (entityInference.getId() == null || entityInference.getId().length() == 0)
            validationException = ValidateActions.addValidationError("missing inference id", validationException);

        if (entityInference.getSearchRequestString() == null || entityInference.getSearchRequestString().length() == 0)
            validationException = ValidateActions.addValidationError("no search request string for inference scheduler specified", validationException);


        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(inferenceId);
        refreshPolicy.writeTo(out);
        out.writeEnum(method);
        entityInference.writeTo(out);
    }

    public String getInferenceId() {
        return inferenceId;
    }

    public RestRequest.Method getMethod() {
        return method;
    }

    public EntityInference getEntityInference() {
        return entityInference;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }
}