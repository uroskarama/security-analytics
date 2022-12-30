package org.opensearch.securityanalytics.ueba;

import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

public class UebaAggregator implements ScheduledJobParameter {
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String ENABLED_TIME_FIELD = "enabled_time";
    public static final String SCHEDULE_FIELD = "schedule";
    public static final String ENABLED_FIELD = "enabled";
    public static final String NO_ID = "";

    private final String id;
    private final Boolean enabled;
    private final Instant lastUpdateTime;
    private final Instant enabledTime;
    private final Schedule schedule;

    public UebaAggregator(String id, Boolean enabled, Instant lastUpdateTime, Instant enabledTime, Schedule schedule, Long seqNo, Long primaryTerm) {
        this.id = id;
        this.enabled = enabled;
        this.lastUpdateTime = lastUpdateTime;
        this.enabledTime = enabledTime;
        this.schedule = schedule;
    }

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
        builder.field(ENABLED_FIELD, enabled);
        builder.field(SCHEDULE_FIELD, schedule);
        if (enabledTime == null) {
            builder.nullField(ENABLED_TIME_FIELD);
        } else {
            builder.timeField(ENABLED_TIME_FIELD, String.format(Locale.getDefault(), "%s_in_millis", ENABLED_TIME_FIELD), enabledTime.toEpochMilli());
        }        if (lastUpdateTime == null) {
            builder.nullField(LAST_UPDATE_TIME_FIELD);
        } else {
            builder.timeField(LAST_UPDATE_TIME_FIELD, String.format(Locale.getDefault(), "%s_in_millis", LAST_UPDATE_TIME_FIELD), lastUpdateTime.toEpochMilli());
        }
        return builder.endObject();
    }

    public static UebaAggregator parse(XContentParser xcp, String id, Long seqNo, Long primaryTerm) throws IOException {
        if (id == null) {
            id = NO_ID;
        }

        Schedule schedule = null;
        Instant lastUpdateTime = null;
        Instant enabledTime = null;
        Boolean enabled = true;

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp);
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = xcp.currentName();
            xcp.nextToken();

            switch (fieldName) {
                case ENABLED_FIELD:
                    enabled = xcp.booleanValue();
                    break;
                case SCHEDULE_FIELD:
                    schedule = ScheduleParser.parse(xcp);
                    break;
                case ENABLED_TIME_FIELD:
                    if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) {
                        enabledTime = null;
                    } else if (xcp.currentToken().isValue()) {
                        enabledTime = Instant.ofEpochMilli(xcp.longValue());
                    } else {
                        XContentParserUtils.throwUnknownToken(xcp.currentToken(), xcp.getTokenLocation());
                        enabledTime = null;
                    }
                    break;
                case LAST_UPDATE_TIME_FIELD:
                    if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) {
                        lastUpdateTime = null;
                    } else if (xcp.currentToken().isValue()) {
                        lastUpdateTime = Instant.ofEpochMilli(xcp.longValue());
                    } else {
                        XContentParserUtils.throwUnknownToken(xcp.currentToken(), xcp.getTokenLocation());
                        lastUpdateTime = null;
                    }
                    break;
            }
        }

        if (enabled && enabledTime == null) {
            enabledTime = Instant.now();
        } else if (!enabled) {
            enabledTime = null;
        }

        return new UebaAggregator(id, enabled, lastUpdateTime, enabledTime, schedule, seqNo, primaryTerm);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UebaAggregator uebaAggregator = (UebaAggregator) o;
        return Objects.equals(id, uebaAggregator.id) && Objects.equals(enabled, uebaAggregator.enabled) && Objects.equals(lastUpdateTime, uebaAggregator.lastUpdateTime) && Objects.equals(enabledTime, uebaAggregator.enabledTime) && Objects.equals(schedule, uebaAggregator.schedule);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, enabled, lastUpdateTime, enabledTime, schedule);
    }
}
