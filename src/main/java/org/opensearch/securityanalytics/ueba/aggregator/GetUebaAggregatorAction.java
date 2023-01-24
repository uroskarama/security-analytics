/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.aggregator;

import org.opensearch.action.ActionType;

public class GetUebaAggregatorAction extends ActionType<GetUebaAggregatorResponse> {

    public static final GetUebaAggregatorAction INSTANCE = new GetUebaAggregatorAction();
    public static final String NAME = "cluster:admin/opensearch/securityanalytics/aggregator/get";

    public GetUebaAggregatorAction() {
        super(NAME, GetUebaAggregatorResponse::new);
    }
}