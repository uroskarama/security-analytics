/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.rest.*;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.securityanalytics.SecurityAnalyticsPlugin;
import org.opensearch.securityanalytics.util.RestHandlerUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

public class RestIndexInferenceAction extends BaseRestHandler {

    private static final Logger log = LogManager.getLogger(RestIndexInferenceAction.class);
    public static final String INFERENCE_ID = "inference_id";

    @Override
    public String getName() {
        return "index_ueba_inference_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
                new Route(RestRequest.Method.POST, SecurityAnalyticsPlugin.UEBA_INFERENCE_BASE_URI),
                new Route(RestRequest.Method.PUT, String.format(Locale.getDefault(),
                        "%s/{%s}",
                        SecurityAnalyticsPlugin.UEBA_INFERENCE_BASE_URI,
                        INFERENCE_ID))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        log.debug(String.format(Locale.getDefault(), "%s %s", request.method(), SecurityAnalyticsPlugin.UEBA_INFERENCE_BASE_URI));

        WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE;
        if (request.hasParam(RestHandlerUtils.REFRESH)) {
            refreshPolicy = WriteRequest.RefreshPolicy.parse(request.param(RestHandlerUtils.REFRESH));
        }

        String id = request.param(INFERENCE_ID, EntityInference.NO_ID);

        XContentParser xcp = request.contentParser();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp);

        EntityInference entityInference = EntityInference.parse(xcp, id, null, null);
        entityInference.setLastUpdateTime(Instant.now());

        IndexEntityInferenceRequest indexEntityInferenceRequest = new IndexEntityInferenceRequest(entityInference.getId(), refreshPolicy, request.method(), entityInference);
        return channel -> client.execute(IndexEntityInferenceAction.INSTANCE, indexEntityInferenceRequest, indexInferenceResponse(channel, request.method()));

    }

    private RestResponseListener<IndexEntityInferenceResponse> indexInferenceResponse(RestChannel channel, RestRequest.Method restMethod) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(IndexEntityInferenceResponse response) throws Exception {
                RestStatus returnStatus = RestStatus.CREATED;
                if (restMethod == RestRequest.Method.PUT) {
                    returnStatus = RestStatus.OK;
                }

                BytesRestResponse restResponse = new BytesRestResponse(returnStatus, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));

                if (restMethod == RestRequest.Method.POST) {
                    String location = String.format(Locale.getDefault(), "%s/%s", SecurityAnalyticsPlugin.UEBA_INFERENCE_BASE_URI, response.getId());
                    restResponse.addHeader("Location", location);
                }

                return restResponse;
            }
        };
    }
}