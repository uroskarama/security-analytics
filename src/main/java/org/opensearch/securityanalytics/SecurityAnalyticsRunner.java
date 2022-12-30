package org.opensearch.securityanalytics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.securityanalytics.ueba.Ueba;
import org.opensearch.securityanalytics.ueba.UebaRunner;

public class SecurityAnalyticsRunner implements ScheduledJobRunner {

    private static final Logger log = LogManager.getLogger(SecurityAnalyticsRunner.class);

    @Override
    public void runJob(ScheduledJobParameter job, JobExecutionContext context) {
        if (job instanceof Ueba) new UebaRunner().runJob(job, context);
        else {
            String errorMessage = "Invalid job type, found " + job.getClass().getName() + " with id: " + context.getJobId();
            log.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
