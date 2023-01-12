package org.opensearch.securityanalytics.ueba.aggregator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.inject.Inject;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.securityanalytics.action.ValidateRulesRequest;

public class UebaAggregatorRunner implements ScheduledJobRunner {
    private static final Logger log = LogManager.getLogger(UebaAggregatorRunner.class);

    private AggregatorService aggregatorService;


    @Inject
    public UebaAggregatorRunner(AggregatorService aggregatorService){
        this.aggregatorService = aggregatorService;
    }

    @Override
    public void runJob(ScheduledJobParameter job, JobExecutionContext context) {
        if (! (job instanceof UebaAggregator)){
            throw new IllegalStateException("Job parameter is not instance of Aggregator, type: " + job.getClass().getCanonicalName());
        }

        aggregatorService.execute((UebaAggregator) job);

        log.debug("Ueba Job executed");
    }
}
