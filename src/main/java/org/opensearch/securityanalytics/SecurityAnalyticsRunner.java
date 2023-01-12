package org.opensearch.securityanalytics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.securityanalytics.ueba.aggregator.UebaAggregator;
import org.opensearch.securityanalytics.ueba.aggregator.UebaAggregatorRunner;
import org.opensearch.securityanalytics.ueba.mlscheduler.UebaMlScheduler;
import org.opensearch.securityanalytics.ueba.mlscheduler.UebaMlSchedulerRunner;

public class SecurityAnalyticsRunner implements ScheduledJobRunner {

    private static final Logger log = LogManager.getLogger(SecurityAnalyticsRunner.class);
    private UebaAggregatorRunner aggregatorRunner;
    private UebaMlSchedulerRunner mlSchedulerRunner;

    private static SecurityAnalyticsRunner INSTANCE;

    protected SecurityAnalyticsRunner(){}

    public static SecurityAnalyticsRunner getJobRunnerInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (SecurityAnalyticsRunner.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new SecurityAnalyticsRunner();
            return INSTANCE;
        }
    }

    @Override
    public void runJob(ScheduledJobParameter job, JobExecutionContext context) {
        if (job instanceof UebaAggregator)
            aggregatorRunner.runJob(job, context);

        else if (job instanceof UebaMlScheduler)
            mlSchedulerRunner.runJob(job, context);

        else {
            String errorMessage = "Invalid job type, found " + job.getClass().getName() + " with id: " + context.getJobId();
            log.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    public UebaAggregatorRunner getAggregatorRunner() {
        return aggregatorRunner;
    }

    public void setAggregatorRunner(UebaAggregatorRunner aggregatorRunner) {
        this.aggregatorRunner = aggregatorRunner;
    }

    public UebaMlSchedulerRunner getMlSchedulerRunner() {
        return mlSchedulerRunner;
    }

    public void setMlSchedulerRunner(UebaMlSchedulerRunner mlSchedulerRunner) {
        this.mlSchedulerRunner = mlSchedulerRunner;
    }
}
