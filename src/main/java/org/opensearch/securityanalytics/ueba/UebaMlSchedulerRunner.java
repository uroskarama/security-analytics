package org.opensearch.securityanalytics.ueba;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.securityanalytics.action.ValidateRulesRequest;

public class UebaMlSchedulerRunner implements ScheduledJobRunner {
    private static final Logger log = LogManager.getLogger(ValidateRulesRequest.class);

    @Override
    public void runJob(ScheduledJobParameter job, JobExecutionContext context) {
        log.debug("Ueba Job executed");
    }
}
