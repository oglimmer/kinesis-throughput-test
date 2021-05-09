package de.oglimmer.kinesis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
@Slf4j
public class DataGenerator {

    private final DataSender dataSender;
    private final RuntimeConfiguration runtimeConfiguration;
    private final RuntimeStatistics runtimeStatistics;
    private final ReqRespVerifier reqRespVerifier;

    private Stream stream;
    private ExecutorService executorService;
    private Origin origin;

    public void start(Stream stream, Origin origin) {
        this.stream = stream;
        this.origin = origin;
        executorService = Executors.newFixedThreadPool(runtimeConfiguration.getDataGenerationThread());
        for (int i = 0; i < runtimeConfiguration.getDataGenerationThread(); i++) {
            executorService.submit(this::run);
        }
        log.info("Started DataGenerator for {} as {}", stream.name(), origin);
    }

    public void run() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
        while (true) {
            BusMessage busMessage = createRequest(origin);
            dataSender.sendData(stream, busMessage, false);
            runtimeStatistics.getStreamStatsMap().get(stream).incRecordGenerated();
            reqRespVerifier.register(busMessage);
            try {
                Thread.sleep(runtimeConfiguration.getDataGenerationSpeed());
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
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
