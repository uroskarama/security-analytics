package org.opensearch.securityanalytics.ueba.aggregator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;

public class AggregatorIndices {

    private static final Logger log = LogManager.getLogger(AggregatorIndices.class);

    private final Client client;

    public AggregatorIndices(Client client) {
        this.client = client;
    }


}
