package de.oglimmer.kinesis;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class DataGenerator {

    private final DataSender dataSender;
    private final RuntimeConfiguration runtimeConfiguration;
    private final RuntimeStatistics runtimeStatistics;
    private final ReqRespVerifier reqRespVerifier;

    @Getter
    private Map<Stream, DataGeneratorStream> data = new HashMap<>();

    public void init(Stream stream, Origin origin) {
        DataGeneratorStream dataGeneratorStream = new DataGeneratorStream();
        data.put(stream, dataGeneratorStream);
        dataGeneratorStream.init(stream, origin);
    }

    class DataGeneratorStream {

        private Stream stream;
        private ScheduledExecutorService executorService;
        private Origin origin;
        private ScheduledFuture<?> scheduledFuture;

        public void init(Stream stream, Origin origin) {
            this.stream = stream;
            this.origin = origin;
            executorService = Executors.newScheduledThreadPool(runtimeConfiguration.getDataGenerationThread());
        }

        public void start() {
            scheduledFuture = executorService.scheduleAtFixedRate(this::run, 100_000,
                    runtimeConfiguration.getDataGenerationSpeed(), TimeUnit.MICROSECONDS);
            log.info("Started DataGenerator for {} as {}", stream.name(), origin);
        }

        public void setDataRate(long rate) {
            stop();
            runtimeConfiguration.setDataGenerationSpeed(rate);
            start();
        }

        public void stop() {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                scheduledFuture = null;
                log.info("Stopped DataGenerator for {} as {}", stream.name(), origin);
            }
        }

        private void run() {
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

}
