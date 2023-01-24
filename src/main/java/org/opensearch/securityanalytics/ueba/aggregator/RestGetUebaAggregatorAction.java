/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.aggregator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.opensearch.rest.RestRequest.Method.GET;


import org.opensearch.securityanalytics.SecurityAnalyticsPlugin;

public class RestGetUebaAggregatorAction extends BaseRestHandler {

    private static final Logger log = LogManager.getLogger(RestGetUebaAggregatorAction.class);

    @Override
    public String getName() {
        return "get_aggregator_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, String.format(Locale.getDefault(), "%s/{%s}", SecurityAnalyticsPlugin.UEBA_AGGREGATOR_BASE_URI, GetUebaAggregatorRequest.AGGREGATOR_ID)));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String aggregatorId = request.param(GetUebaAggregatorRequest.AGGREGATOR_ID, UebaAggregator.NO_ID);

        if (aggregatorId == null || aggregatorId.isEmpty()) {
            throw new IllegalArgumentException("missing id");
        }

        GetUebaAggregatorRequest req = new GetUebaAggregatorRequest(aggregatorId, RestActions.parseVersion(request));

        return channel -> client.execute(
                GetUebaAggregatorAction.INSTANCE,
                req,
                new RestToXContentListener<>(channel)
        );
    }
}
