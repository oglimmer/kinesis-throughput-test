package de.oglimmer.kinesis;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class ReqRespVerifier {

    private final RuntimeStatistics runtimeStatistics;

    @Getter
    private Map<UUID, BusMessage> requestUUIDs = new ConcurrentHashMap<>();

    public void register(BusMessage request) {
        log.debug("Adding req {}", request.getUuid());
        requestUUIDs.put(request.getUuid(), request);
    }

    public void unregister(BusMessage response) {
        if (!requestUUIDs.containsKey(response.getUuid())) {
            log.error("Failed to find req for {}", response.getUuid());
        } else {
            final BusMessage request = requestUUIDs.remove(response.getUuid());
            long processingTime = response.getCreationTime().toEpochMilli() - request.getCreationTime().toEpochMilli();
            log.debug("Message {} took {} millis", response.getUuid(), processingTime);
            runtimeStatistics.getOriginStatsMap().get(response.getOrigin()).addRoundTripTime(processingTime);
        }
    }

}
