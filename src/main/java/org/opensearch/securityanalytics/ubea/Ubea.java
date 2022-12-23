package org.opensearch.securityanalytics.ubea;

import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.Schedule;

import java.io.IOException;
import java.time.Instant;

public class Ubea implements ScheduledJobParameter {
    private String id;
    private Boolean enabled;
    private Instant lastUpdateTime;
    private Instant enabledTime;
    private Schedule schedule;

    @Override
    public String getName() {
        return id;
    }

    @Override
    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public Instant getEnabledTime() {
        return enabledTime;
    }

    @Override
    public Schedule getSchedule() {
        return schedule;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        return builder.endObject();
    }

    public Ubea parse(XContentParser xcp, String id, Long seqNo, Long primaryTerm) {
        return new Ubea();
    }
}
