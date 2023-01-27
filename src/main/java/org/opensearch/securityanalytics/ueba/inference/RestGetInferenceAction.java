/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
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

import static org.opensearch.rest.RestRequest.Method.GET;

public class RestGetInferenceAction extends BaseRestHandler {

    private static final Logger log = LogManager.getLogger(RestGetInferenceAction.class);

    @Override
    public String getName() {
        return "get_inference_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, String.format(Locale.getDefault(), "%s/{%s}", SecurityAnalyticsPlugin.UEBA_INFERENCE_BASE_URI, GetInferenceRequest.INFERENCE_ID)));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String inferenceId = request.param(GetInferenceRequest.INFERENCE_ID, EntityInference.NO_ID);

        if (inferenceId == null || inferenceId.isEmpty()) {
            throw new IllegalArgumentException("missing id");
        }

        GetInferenceRequest req = new GetInferenceRequest(inferenceId, RestActions.parseVersion(request));

        return channel -> client.execute(
                GetInferenceAction.INSTANCE,
                req,
                new RestToXContentListener<>(channel)
        );
    }
}
