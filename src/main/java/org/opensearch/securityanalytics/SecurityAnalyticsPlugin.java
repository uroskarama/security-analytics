/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.securityanalytics;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.component.LifecycleComponent;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.jobscheduler.spi.JobSchedulerExtension;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.securityanalytics.action.AckAlertsAction;
import org.opensearch.securityanalytics.action.CreateIndexMappingsAction;
import org.opensearch.securityanalytics.action.DeleteDetectorAction;
import org.opensearch.securityanalytics.action.GetAlertsAction;
import org.opensearch.securityanalytics.action.GetDetectorAction;
import org.opensearch.securityanalytics.action.GetFindingsAction;
import org.opensearch.securityanalytics.action.GetIndexMappingsAction;
import org.opensearch.securityanalytics.action.GetMappingsViewAction;
import org.opensearch.securityanalytics.action.IndexDetectorAction;
import org.opensearch.securityanalytics.action.SearchDetectorAction;
import org.opensearch.securityanalytics.action.UpdateIndexMappingsAction;
import org.opensearch.securityanalytics.indexmanagment.DetectorIndexManagementService;
import org.opensearch.securityanalytics.action.ValidateRulesAction;
import org.opensearch.securityanalytics.mapper.MapperService;
import org.opensearch.securityanalytics.resthandler.RestAcknowledgeAlertsAction;
import org.opensearch.securityanalytics.resthandler.RestGetFindingsAction;
import org.opensearch.securityanalytics.resthandler.RestValidateRulesAction;
import org.opensearch.securityanalytics.transport.TransportAcknowledgeAlertsAction;
import org.opensearch.securityanalytics.transport.TransportCreateIndexMappingsAction;
import org.opensearch.securityanalytics.transport.TransportGetFindingsAction;
import org.opensearch.securityanalytics.action.DeleteRuleAction;
import org.opensearch.securityanalytics.action.IndexRuleAction;
import org.opensearch.securityanalytics.action.SearchRuleAction;
import org.opensearch.securityanalytics.model.Rule;
import org.opensearch.securityanalytics.resthandler.RestDeleteDetectorAction;
import org.opensearch.securityanalytics.resthandler.RestDeleteRuleAction;
import org.opensearch.securityanalytics.resthandler.RestIndexRuleAction;
import org.opensearch.securityanalytics.resthandler.RestSearchRuleAction;
import org.opensearch.securityanalytics.transport.TransportDeleteRuleAction;
import org.opensearch.securityanalytics.transport.TransportIndexRuleAction;
import org.opensearch.securityanalytics.transport.TransportSearchRuleAction;
import org.opensearch.securityanalytics.transport.TransportUpdateIndexMappingsAction;
import org.opensearch.securityanalytics.transport.TransportGetIndexMappingsAction;
import org.opensearch.securityanalytics.model.Detector;
import org.opensearch.securityanalytics.model.DetectorInput;
import org.opensearch.securityanalytics.resthandler.RestCreateIndexMappingsAction;
import org.opensearch.securityanalytics.resthandler.RestGetAlertsAction;
import org.opensearch.securityanalytics.resthandler.RestGetDetectorAction;
import org.opensearch.securityanalytics.resthandler.RestGetIndexMappingsAction;
import org.opensearch.securityanalytics.resthandler.RestGetMappingsViewAction;
import org.opensearch.securityanalytics.resthandler.RestIndexDetectorAction;
import org.opensearch.securityanalytics.resthandler.RestSearchDetectorAction;
import org.opensearch.securityanalytics.resthandler.RestUpdateIndexMappingsAction;
import org.opensearch.securityanalytics.settings.SecurityAnalyticsSettings;
import org.opensearch.securityanalytics.transport.TransportDeleteDetectorAction;
import org.opensearch.securityanalytics.transport.TransportGetAlertsAction;
import org.opensearch.securityanalytics.transport.TransportGetDetectorAction;
import org.opensearch.securityanalytics.transport.TransportGetMappingsViewAction;
import org.opensearch.securityanalytics.transport.TransportIndexDetectorAction;
import org.opensearch.securityanalytics.transport.TransportSearchDetectorAction;
import org.opensearch.securityanalytics.transport.TransportValidateRulesAction;
import org.opensearch.securityanalytics.ueba.aggregator.*;
import org.opensearch.securityanalytics.ueba.mlscheduler.UebaMlSchedulerRunner;
import org.opensearch.securityanalytics.util.DetectorIndices;
import org.opensearch.securityanalytics.util.RuleIndices;
import org.opensearch.securityanalytics.util.RuleTopicIndices;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

public class SecurityAnalyticsPlugin extends Plugin implements ActionPlugin, JobSchedulerExtension {

    public static final String PLUGINS_BASE_URI = "/_plugins/_security_analytics";
    public static final String MAPPER_BASE_URI = PLUGINS_BASE_URI + "/mappings";
    public static final String MAPPINGS_VIEW_BASE_URI = MAPPER_BASE_URI + "/view";
    public static final String FINDINGS_BASE_URI = PLUGINS_BASE_URI + "/findings";
    public static final String ALERTS_BASE_URI = PLUGINS_BASE_URI + "/alerts";
    public static final String DETECTOR_BASE_URI = PLUGINS_BASE_URI + "/detectors";
    public static final String RULE_BASE_URI = PLUGINS_BASE_URI + "/rules";

    public static final String UEBA_AGGREGATOR_BASE_URI = PLUGINS_BASE_URI + "/ueba/aggregator";
    public static final String SECURITY_ANALYTICS_JOB_INDEX = ".opendistro-sa-config";
    public static final String SECURITY_ANALYTICS_JOB_TYPE = "opendistro-security-analytics";

    private DetectorIndices detectorIndices;

    private RuleTopicIndices ruleTopicIndices;

    private MapperService mapperService;

    private RuleIndices ruleIndices;

    private AggregatorIndices aggregatorIndices;

    private AggregatorService aggregatorService;

    private DetectorIndexManagementService detectorIndexManagementService;

    @Override
    public String getJobType() {
        return SECURITY_ANALYTICS_JOB_TYPE;
    }

    @Override
    public String getJobIndex() {
        return SECURITY_ANALYTICS_JOB_INDEX;
    }

    @Override
    public ScheduledJobRunner getJobRunner() {
        return SecurityAnalyticsRunner.getJobRunnerInstance();
    }

    @Override
    public ScheduledJobParser getJobParser() {
        return (xContentParser, id, jobDocVersion) -> UebaAggregator.parse(xContentParser, id, jobDocVersion.getSeqNo(), jobDocVersion.getPrimaryTerm());
    }

    @Override
    public Collection<Object> createComponents(Client client,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService,
                                               ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry,
                                               Environment environment,
                                               NodeEnvironment nodeEnvironment,
                                               NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        detectorIndices = new DetectorIndices(client.admin(), clusterService, threadPool);
        ruleTopicIndices = new RuleTopicIndices(client, clusterService);
        mapperService = new MapperService(client.admin().indices());
        ruleIndices = new RuleIndices(client, clusterService, threadPool);
        aggregatorIndices = new AggregatorIndices(client.admin(), clusterService, threadPool);
        aggregatorService = new AggregatorService(client, xContentRegistry);

        SecurityAnalyticsRunner securityAnalyticsRunner = SecurityAnalyticsRunner.getJobRunnerInstance();
        securityAnalyticsRunner.setAggregatorRunner(new UebaAggregatorRunner(aggregatorService));
        securityAnalyticsRunner.setMlSchedulerRunner(new UebaMlSchedulerRunner());

        return List.of(detectorIndices, ruleTopicIndices, ruleIndices, mapperService, aggregatorIndices, aggregatorService);
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Collections.singletonList(DetectorIndexManagementService.class);
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings,
                                             RestController restController,
                                             ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings,
                                             SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return List.of(
                new RestAcknowledgeAlertsAction(),
                new RestUpdateIndexMappingsAction(),
                new RestCreateIndexMappingsAction(),
                new RestGetIndexMappingsAction(),
                new RestIndexDetectorAction(),
                new RestGetDetectorAction(),
                new RestSearchDetectorAction(),
                new RestDeleteDetectorAction(),
                new RestGetFindingsAction(),
                new RestGetMappingsViewAction(),
                new RestGetAlertsAction(),
                new RestIndexRuleAction(),
                new RestSearchRuleAction(),
                new RestDeleteRuleAction(),
                new RestValidateRulesAction(),
                new RestIndexUebaAggregatorAction()
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
                Detector.XCONTENT_REGISTRY,
                DetectorInput.XCONTENT_REGISTRY,
                Rule.XCONTENT_REGISTRY,
                UebaAggregator.XCONTENT_REGISTRY
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
                SecurityAnalyticsSettings.INDEX_TIMEOUT,
                SecurityAnalyticsSettings.FILTER_BY_BACKEND_ROLES,
                SecurityAnalyticsSettings.ALERT_HISTORY_ENABLED,
                SecurityAnalyticsSettings.ALERT_HISTORY_ROLLOVER_PERIOD,
                SecurityAnalyticsSettings.ALERT_HISTORY_INDEX_MAX_AGE,
                SecurityAnalyticsSettings.ALERT_HISTORY_MAX_DOCS,
                SecurityAnalyticsSettings.ALERT_HISTORY_RETENTION_PERIOD,
                SecurityAnalyticsSettings.REQUEST_TIMEOUT,
                SecurityAnalyticsSettings.MAX_ACTION_THROTTLE_VALUE,
                SecurityAnalyticsSettings.FINDING_HISTORY_ENABLED,
                SecurityAnalyticsSettings.FINDING_HISTORY_MAX_DOCS,
                SecurityAnalyticsSettings.FINDING_HISTORY_INDEX_MAX_AGE,
                SecurityAnalyticsSettings.FINDING_HISTORY_ROLLOVER_PERIOD,
                SecurityAnalyticsSettings.FINDING_HISTORY_RETENTION_PERIOD
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
                new ActionPlugin.ActionHandler<>(AckAlertsAction.INSTANCE, TransportAcknowledgeAlertsAction.class),
                new ActionPlugin.ActionHandler<>(UpdateIndexMappingsAction.INSTANCE, TransportUpdateIndexMappingsAction.class),
                new ActionPlugin.ActionHandler<>(CreateIndexMappingsAction.INSTANCE, TransportCreateIndexMappingsAction.class),
                new ActionPlugin.ActionHandler<>(GetIndexMappingsAction.INSTANCE, TransportGetIndexMappingsAction.class),
                new ActionPlugin.ActionHandler<>(IndexDetectorAction.INSTANCE, TransportIndexDetectorAction.class),
                new ActionPlugin.ActionHandler<>(DeleteDetectorAction.INSTANCE, TransportDeleteDetectorAction.class),
                new ActionPlugin.ActionHandler<>(GetMappingsViewAction.INSTANCE, TransportGetMappingsViewAction.class),
                new ActionPlugin.ActionHandler<>(GetDetectorAction.INSTANCE, TransportGetDetectorAction.class),
                new ActionPlugin.ActionHandler<>(SearchDetectorAction.INSTANCE, TransportSearchDetectorAction.class),
                new ActionPlugin.ActionHandler<>(GetFindingsAction.INSTANCE, TransportGetFindingsAction.class),
                new ActionPlugin.ActionHandler<>(GetAlertsAction.INSTANCE, TransportGetAlertsAction.class),
                new ActionPlugin.ActionHandler<>(IndexRuleAction.INSTANCE, TransportIndexRuleAction.class),
                new ActionPlugin.ActionHandler<>(SearchRuleAction.INSTANCE, TransportSearchRuleAction.class),
                new ActionPlugin.ActionHandler<>(DeleteRuleAction.INSTANCE, TransportDeleteRuleAction.class),
                new ActionPlugin.ActionHandler<>(ValidateRulesAction.INSTANCE, TransportValidateRulesAction.class),
                new ActionPlugin.ActionHandler<>(IndexUebaAggregatorAction.INSTANCE, TransportIndexUebaAggregatorAction.class)
        );
    }
}