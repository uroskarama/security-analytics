package org.opensearch.securityanalytics.ueba.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.securityanalytics.SecurityAnalyticsPlugin;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.opensearch.rest.RestRequest.Method.POST;

public class RestExecuteInferenceAction extends BaseRestHandler {

    private static final Logger log = LogManager.getLogger(RestExecuteInferenceAction.class);

    @Override
    public String getName() {
        return "execute_inference_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, String.format(Locale.getDefault(), "%s/{%s}/_execute", SecurityAnalyticsPlugin.UEBA_INFERENCE_BASE_URI, ExecuteInferenceRequest.INFERENCE_ID)));

    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String inferenceId = request.param(ExecuteInferenceRequest.INFERENCE_ID, EntityInference.NO_ID);

        if (inferenceId == null || inferenceId.isEmpty()) {
            throw new IllegalArgumentException("missing id");
        }

        ExecuteInferenceRequest req = new ExecuteInferenceRequest(inferenceId, RestActions.parseVersion(request));

        return channel -> client.execute(
                ExecuteInferenceAction.INSTANCE,
                req,
                new RestToXContentListener<>(channel)
        );
    }
}
