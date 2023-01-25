package org.opensearch.securityanalytics.ueba.aggregator;

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

public class RestExecuteAggregatorAction extends BaseRestHandler {

    private static final Logger log = LogManager.getLogger(RestExecuteAggregatorAction.class);

    @Override
    public String getName() {
        return "execute_aggregator_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, String.format(Locale.getDefault(), "%s/{%s}/_execute", SecurityAnalyticsPlugin.UEBA_AGGREGATOR_BASE_URI, ExecuteAggregatorRequest.AGGREGATOR_ID)));

    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String aggregatorId = request.param(ExecuteAggregatorRequest.AGGREGATOR_ID, UebaAggregator.NO_ID);

        if (aggregatorId == null || aggregatorId.isEmpty()) {
            throw new IllegalArgumentException("missing id");
        }

        ExecuteAggregatorRequest req = new ExecuteAggregatorRequest(aggregatorId, RestActions.parseVersion(request));

        return channel -> client.execute(
                ExecuteAggregatorAction.INSTANCE,
                req,
                new RestToXContentListener<>(channel)
        );
    }
}
