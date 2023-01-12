/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.aggregator;

import org.opensearch.action.ActionType;
import org.opensearch.securityanalytics.action.IndexDetectorResponse;

public class IndexUebaAggregatorAction extends ActionType<IndexUebaAggregatorResponse> {

    public static final IndexUebaAggregatorAction INSTANCE = new IndexUebaAggregatorAction();
    public static final String NAME = "cluster:admin/opensearch/securityanalytics/ueba-aggregator/write";

    public IndexUebaAggregatorAction() {
        super(NAME, IndexUebaAggregatorResponse::new);
    }
}