package de.oglimmer.kinesis;

import lombok.Getter;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Getter
@Configuration
@EnableScheduling
public class RuntimeStatistics {

    private static final int MAX_ITEMS_IN_HISTORY = 10;

    private Map<Stream, StreamStats> streamStatsMap = new HashMap<>();
    private Map<Origin, OriginStats> originStatsMap = new HashMap<>();

    @PostConstruct
    public void init() {
        Arrays.stream(Stream.values()).forEach(e -> streamStatsMap.put(e, new StreamStats()));
        Arrays.stream(Origin.values()).forEach(e -> originStatsMap.put(e, new OriginStats()));
    }

    @Scheduled(cron = "0 * * * * *")
    public void scheduled() {
        streamStatsMap.values().forEach(e -> {
            if (e.recordsGeneratedPerMinute.size() > MAX_ITEMS_IN_HISTORY) {
                e.recordsGeneratedPerMinute.remove(0);
            }
            e.recordsGeneratedPerMinute.add(e.recordsGeneratedLastMinute);
            e.recordsGeneratedLastMinute = 0;

            if (e.recordsAnsweredPerMinute.size() > MAX_ITEMS_IN_HISTORY) {
                e.recordsAnsweredPerMinute.remove(0);
            }
            e.recordsAnsweredPerMinute.add(e.recordsAnsweredLastMinute);
            e.recordsAnsweredLastMinute = 0;

        });
        originStatsMap.values().forEach(e -> {
            e.totalRoundTripLastMinuteCounter = 0;
            e.totalRountTripLastMinuteTime = 0;
        });
    }

    @Getter
    class OriginStats {
        private long totalRoundTripLastMinuteCounter;
        private long totalRountTripLastMinuteTime;

        public void addRoundTripTime(long processingTime) {
            totalRoundTripLastMinuteCounter++;
            totalRountTripLastMinuteTime += processingTime;
        }

        public double getRoundTripTimeLastMinuteAvg() {
            if (totalRoundTripLastMinuteCounter == 0) {
                return -1;
            }
            return totalRountTripLastMinuteTime / totalRoundTripLastMinuteCounter;
        }
    }

    @Getter
    class StreamStats {

        private long recordsGenerated;
        private long recordsAnswered;

        private List<Long> recordsGeneratedPerMinute = Collections.synchronizedList(new ArrayList());
        private List<Long> recordsAnsweredPerMinute = Collections.synchronizedList(new ArrayList());

        private long recordsGeneratedLastMinute;
        private long recordsAnsweredLastMinute;

        public void incRecordGenerated() {
            recordsGenerated++;
            recordsGeneratedLastMinute++;
        }

        public void incRecordAnswere() {
            recordsAnswered++;
            recordsAnsweredLastMinute++;
        }

    }
}
