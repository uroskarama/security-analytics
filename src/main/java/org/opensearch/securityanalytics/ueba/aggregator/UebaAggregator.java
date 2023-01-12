package org.opensearch.securityanalytics.ueba.aggregator;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser;
import java.io.IOException;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

public class UebaAggregator implements ScheduledJobParameter, Writeable, ToXContentObject {
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String ENABLED_TIME_FIELD = "enabled_time";
    public static final String SCHEDULE_FIELD = "schedule";
    public static final String ENABLED_FIELD = "enabled";
    public static final String NO_ID = "";
    public static final String SEARCH_REQUEST_STRING_FIELD = "search_request_string";
    public static final String ENTITY_INDEX_FIELD = "entity_index";
    public static final String ENTITY_FIELD_NAME_FIELD = "entity_field";

    public static final String SOURCE_INDEX_FIELD = "source_index";

    public static final String BATCH_SIZE_FIELD = "batch_size";

    private String id;
    private Boolean enabled;
    private Instant lastUpdateTime;
    private Instant enabledTime;
    private Schedule schedule;

    private Long seqNo;

    private Long primaryTerm;

    private String searchRequestString;

    private String sourceIndex;

    private Long batchSize;

    private String entityIndex;

    private String entityFieldName;

    public UebaAggregator(String id,
                          Boolean enabled,
                          Instant lastUpdateTime,
                          Instant enabledTime,
                          Schedule schedule,
                          Long seqNo,
                          Long primaryTerm,
                          String searchRequestString,
                          String sourceIndex,
                          Long batchSize,
                          String entityIndex,
                          String entityFieldName) {
        this.id = id;
        this.enabled = enabled;
        this.lastUpdateTime = lastUpdateTime;
        this.enabledTime = enabledTime;
        this.schedule = schedule;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.searchRequestString = searchRequestString;
        this.sourceIndex = sourceIndex;
        this.batchSize = batchSize;
        this.entityIndex = entityIndex;
        this.entityFieldName = entityFieldName;
    }

    public UebaAggregator(StreamInput sin) throws IOException {
        this(
            sin.readString(),
            sin.readBoolean(),
            sin.readInstant(),
            sin.readInstant(),
            new IntervalSchedule(sin),  // FIXME: Should we cover CronSchedule too?
            sin.readLong(),
            sin.readLong(),
            sin.readString(),
            sin.readString(),
            sin.readLong(),
            sin.readString(),
            sin.readString()
        );
    }

    public static UebaAggregator readFrom(StreamInput sin) throws IOException {
        return new UebaAggregator(sin);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeBoolean(enabled);
        out.writeInstant(lastUpdateTime);
        out.writeInstant(enabledTime);
        schedule.writeTo(out); // FIXME should we cover both CRON and INTERVAL schedule?
        out.writeLong(seqNo);
        out.writeLong(primaryTerm);
        out.writeString(searchRequestString);
        out.writeString(sourceIndex);
        out.writeLong(batchSize);
        out.writeString(entityIndex);
        out.writeString(entityFieldName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ENABLED_FIELD, enabled);
        builder.field(SCHEDULE_FIELD, schedule);

        if (lastUpdateTime == null) {
            builder.nullField(LAST_UPDATE_TIME_FIELD);
        } else {
            builder.timeField(LAST_UPDATE_TIME_FIELD, String.format(Locale.getDefault(), "%s_in_millis", LAST_UPDATE_TIME_FIELD), lastUpdateTime.toEpochMilli());
        }

        if (enabledTime == null) {
            builder.nullField(ENABLED_TIME_FIELD);
        } else {
            builder.timeField(ENABLED_TIME_FIELD, String.format(Locale.getDefault(), "%s_in_millis", ENABLED_TIME_FIELD), enabledTime.toEpochMilli());
        }

        builder.field(SEARCH_REQUEST_STRING_FIELD, searchRequestString);
        builder.field(SOURCE_INDEX_FIELD, sourceIndex);
        builder.field(BATCH_SIZE_FIELD, batchSize);
        builder.field(ENTITY_INDEX_FIELD, entityIndex);
        builder.field(ENTITY_FIELD_NAME_FIELD, entityFieldName);

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
        String searchRequestString = null;
        String sourceIndex = null;
        Long batchSize = 0L;
        String entityIndex = null;
        String entityFieldName =  null;

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
                case SEARCH_REQUEST_STRING_FIELD:
                    searchRequestString = xcp.text();
                    break;
                case SOURCE_INDEX_FIELD:
                    sourceIndex = xcp.text();
                case BATCH_SIZE_FIELD:
                    batchSize = xcp.longValue();
                case ENTITY_INDEX_FIELD:
                    entityIndex = xcp.text();
                    break;
                case ENTITY_FIELD_NAME_FIELD:
                    entityFieldName = xcp.text();
                    break;
            }
        }

        if (enabled && enabledTime == null) {
            enabledTime = Instant.now();
        } else if (!enabled) {
            enabledTime = null;
        }

        return new UebaAggregator(id,
                enabled,
                lastUpdateTime,
                enabledTime,
                schedule,
                seqNo,
                primaryTerm,
                searchRequestString,
                sourceIndex,
                batchSize,
                entityIndex,
                entityFieldName);
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

    public void setLastUpdateTime(Instant lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public void setEnabledTime(Instant enabledTime) {
        this.enabledTime = enabledTime;
    }

    public void setSchedule(Schedule schedule) {
        this.schedule = schedule;
    }

    public Long getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(Long seqNo) {
        this.seqNo = seqNo;
    }

    public Long getPrimaryTerm() {
        return primaryTerm;
    }

    public void setPrimaryTerm(Long primaryTerm) {
        this.primaryTerm = primaryTerm;
    }

    public String getSearchRequestString() {
        return searchRequestString;
    }

    public void setSearchRequestString(String searchRequestString) {
        this.searchRequestString = searchRequestString;
    }

    public String getEntityIndex() {
        return entityIndex;
    }

    public void setEntityIndex(String entityIndex) {
        this.entityIndex = entityIndex;
    }

    public String getEntityFieldName() {
        return entityFieldName;
    }

    public void setEntityFieldName(String entityFieldName) {
        this.entityFieldName = entityFieldName;
    }

    public String getSourceIndex() {
        return sourceIndex;
    }

    public void setSourceIndex(String sourceIndex) {
        this.sourceIndex = sourceIndex;
    }

    public Long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Long batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public boolean equals(Object o) {   // FIXME
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UebaAggregator uebaAggregator = (UebaAggregator) o;
        return Objects.equals(id, uebaAggregator.id) && Objects.equals(enabled, uebaAggregator.enabled) && Objects.equals(lastUpdateTime, uebaAggregator.lastUpdateTime) && Objects.equals(enabledTime, uebaAggregator.enabledTime) && Objects.equals(schedule, uebaAggregator.schedule);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, enabled, lastUpdateTime, enabledTime, schedule);
    } // FIXME


}
