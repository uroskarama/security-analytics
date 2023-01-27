package org.opensearch.securityanalytics.ueba.core;

import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.securityanalytics.SecurityAnalyticsPlugin;

import java.time.Instant;

public interface UEBAJobParameter extends ScheduledJobParameter, Writeable, ToXContentObject {
    String LAST_UPDATE_TIME_FIELD = "last_update_time";
    String ENABLED_TIME_FIELD = "enabled_time";
    String SCHEDULE_FIELD = "schedule";
    String ENABLED_FIELD = "enabled";
    String NO_ID = "";
    String SEARCH_REQUEST_STRING_FIELD = "search_request_string";
    String ENTITY_INDEX_FIELD = "entity_index";
    String SOURCE_INDEX_FIELD = "source_index";
    String BATCH_SIZE_FIELD = "batch_size";

    static String jobParameterIndex() {
        return SecurityAnalyticsPlugin.SECURITY_ANALYTICS_JOB_INDEX;
    }

    void setLastUpdateTime(Instant lastUpdateTime);

    String getId();
    String getType();
    Boolean getEnabled();

    String getSearchRequestString();

    String getSourceIndex();

    Integer getPageSize();

    String getEntityIndex();
}
