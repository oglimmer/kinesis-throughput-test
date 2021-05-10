package de.oglimmer.kinesis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class DataGenerator {

    private final DataSender dataSender;
    private final RuntimeConfiguration runtimeConfiguration;
    private final RuntimeStatistics runtimeStatistics;
    private final ReqRespVerifier reqRespVerifier;

    private Stream stream;
    private ScheduledExecutorService executorService;
    private Origin origin;

    public void start(Stream stream, Origin origin) {
        this.stream = stream;
        this.origin = origin;
        executorService = Executors.newScheduledThreadPool(runtimeConfiguration.getDataGenerationThread());
        executorService.scheduleAtFixedRate(this::run, 30_000, runtimeConfiguration.getDataGenerationSpeed(),
                TimeUnit.MILLISECONDS);
        log.info("Started DataGenerator for {} as {}", stream.name(), origin);
    }

    public void run() {
        BusMessage busMessage = createRequest(origin);
        dataSender.sendData(stream, busMessage);
        runtimeStatistics.getStreamStatsMap().get(stream).incRecordGenerated();
        reqRespVerifier.register(busMessage);
    }

    private BusMessage createRequest(Origin origin) {
        return new BusMessage(origin, generatePayload());
    }

    private String generatePayload() {
        StringBuilder buff = new StringBuilder(runtimeConfiguration.getPayloadSize());
        for (int i = 0; i < runtimeConfiguration.getPayloadSize(); i++) {
            buff.append((char) (Math.random() * 26 + 65));
        }
        return buff.toString();
    }

}
