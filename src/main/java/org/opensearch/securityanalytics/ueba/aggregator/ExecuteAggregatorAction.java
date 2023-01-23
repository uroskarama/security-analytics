/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.aggregator;

import org.opensearch.action.ActionType;

public class ExecuteAggregatorAction extends ActionType<ExecuteAggregatorResponse> {

    public static final ExecuteAggregatorAction INSTANCE = new ExecuteAggregatorAction();
    public static final String NAME = "cluster:admin/opensearch/securityanalytics/aggregator/execute";

    public ExecuteAggregatorAction() {
        super(NAME, ExecuteAggregatorResponse::new);
    }
}