package org.opensearch.securityanalytics.ueba.inference;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Locale;

import static org.opensearch.action.ValidateActions.addValidationError;

public class ExecuteInferenceRequest extends ActionRequest {

    private final String inferenceId;

    private final Long version;

    public static final String INFERENCE_ID = "inference_id";

    public ExecuteInferenceRequest(String inferenceId, Long version){
        this.inferenceId = inferenceId;
        this.version = version;
    }

    public ExecuteInferenceRequest(StreamInput sin) throws IOException {
        this(sin.readString(), sin.readLong());
    }

        @Override
    public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (inferenceId == null || inferenceId.length() == 0) {
                validationException = addValidationError(String.format(Locale.getDefault(), "%s is missing", INFERENCE_ID), null);
            }
            return validationException;
        }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(inferenceId);
        out.writeLong(version);
    }

    public String getInferenceId() {
        return inferenceId;
    }

    public Long getVersion() {
        return version;
    }
}