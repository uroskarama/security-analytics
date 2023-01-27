/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.inference;

import org.opensearch.action.ActionType;

public class IndexEntityInferenceAction extends ActionType<IndexEntityInferenceResponse> {

    public static final IndexEntityInferenceAction INSTANCE = new IndexEntityInferenceAction();
    public static final String NAME = "cluster:admin/opensearch/securityanalytics/inference/write";

    public IndexEntityInferenceAction() {
        super(NAME, IndexEntityInferenceResponse::new);
    }
}