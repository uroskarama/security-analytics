/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.inference;

import org.opensearch.action.ActionType;

public class ExecuteInferenceAction extends ActionType<ExecuteInferenceResponse> {

    public static final ExecuteInferenceAction INSTANCE = new ExecuteInferenceAction();
    public static final String NAME = "cluster:admin/opensearch/securityanalytics/inference/execute";

    public ExecuteInferenceAction() {
        super(NAME, ExecuteInferenceResponse::new);
    }
}