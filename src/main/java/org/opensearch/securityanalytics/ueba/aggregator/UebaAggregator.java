package org.opensearch.securityanalytics.ueba.aggregator;

import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser;
import org.opensearch.securityanalytics.model.Value;
import org.opensearch.securityanalytics.ueba.core.UEBAJobParameter;

import java.io.IOException;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

public class UebaAggregator implements UEBAJobParameter {

    public static final String AGGREGATOR_TYPE = "aggregator";
    public static final String ENTITY_FIELD_NAME_FIELD = "entity_field_name";

    private final String id;
    private final Boolean enabled;
    private Instant lastUpdateTime;
    private final Instant enabledTime;
    private final Schedule schedule;

    private final Long seqNo;

    private final Long primaryTerm;

    private final Value searchRequestString;

    private final String sourceIndex;

    private final Integer pageSize;

    private final String entityIndex;

    private final String entityFieldName;

    public UebaAggregator(String id,
                          Boolean enabled,
                          Instant lastUpdateTime,
                          Instant enabledTime,
                          Schedule schedule,
                          Long seqNo,
                          Long primaryTerm,
                          String searchRequestString,
                          String sourceIndex,
                          Integer pageSize,
                          String entityIndex,
                          String entityFieldName) {
        this.id = id;
        this.enabled = enabled;
        this.lastUpdateTime = lastUpdateTime;
        this.enabledTime = enabledTime;
        this.schedule = schedule;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.searchRequestString = new Value(searchRequestString);
        this.sourceIndex = sourceIndex;
        this.pageSize = pageSize;
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
            sin.readInt(),
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
        out.writeString(searchRequestString.getValue());
        out.writeString(sourceIndex);
        out.writeInt(pageSize);
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
        builder.field(BATCH_SIZE_FIELD, pageSize);
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
        Value searchRequestString = null;
        String sourceIndex = null;
        Integer batchSize = 0;
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
                    searchRequestString = Value.parse(xcp);
                    break;
                case SOURCE_INDEX_FIELD:
                    sourceIndex = xcp.text();
                    break;
                case BATCH_SIZE_FIELD:
                    batchSize = xcp.intValue();
                    break;
                case ENTITY_INDEX_FIELD:
                    entityIndex = xcp.text();
                    break;
                case ENTITY_FIELD_NAME_FIELD:
                    entityFieldName = xcp.text();
                    break;
            }
        }

        return new UebaAggregator(id,
                enabled,
                lastUpdateTime,
                enabledTime,
                schedule,
                seqNo,
                primaryTerm,
                searchRequestString.getValue(),
                sourceIndex,
                batchSize,
                entityIndex,
                entityFieldName);
    }

    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
            UebaAggregator.class,
            new ParseField(AGGREGATOR_TYPE),
            xcp -> parse(xcp, null, null, null)
    );

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
    public void setLastUpdateTime(Instant lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getType() {
        return AGGREGATOR_TYPE;
    }

    @Override
    public Boolean getEnabled() {
        return enabled;
    }

    @Override
    public String getSearchRequestString() {
        return searchRequestString.getValue();
    }

    @Override
    public String getSourceIndex() {
        return sourceIndex;
    }

    @Override
    public Integer getPageSize() {
        return pageSize;
    }

    @Override
    public String getEntityIndex() {
        return entityIndex;
    }

    public String getEntityFieldName() {
        return entityFieldName;
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

    @Override
    public String toString() {
        return "UebaAggregator{" +
                "id='" + id + '\'' +
                ", enabled=" + enabled +
                ", lastUpdateTime=" + lastUpdateTime +
                ", enabledTime=" + enabledTime +
                ", schedule=" + schedule +
                ", seqNo=" + seqNo +
                ", primaryTerm=" + primaryTerm +
                ", searchRequestString=" + searchRequestString +
                ", sourceIndex='" + sourceIndex + '\'' +
                ", pageSize=" + pageSize +
                ", entityIndex='" + entityIndex + '\'' +
                ", entityFieldName='" + entityFieldName + '\'' +
                '}';
    }
}
