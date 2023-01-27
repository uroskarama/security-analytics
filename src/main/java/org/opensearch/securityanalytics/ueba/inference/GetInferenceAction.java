/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics.ueba.inference;

import org.opensearch.action.ActionType;

public class GetInferenceAction extends ActionType<GetInferenceResponse> {

    public static final GetInferenceAction INSTANCE = new GetInferenceAction();
    public static final String NAME = "cluster:admin/opensearch/securityanalytics/inference/get";

    public GetInferenceAction() {
        super(NAME, GetInferenceResponse::new);
    }
}