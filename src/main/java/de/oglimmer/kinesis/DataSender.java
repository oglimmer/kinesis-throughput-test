package de.oglimmer.kinesis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.SdkBytes;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
@RequiredArgsConstructor
@Component
public class DataSender {

    private final KinesisClient kinesis;
    private final RuntimeConfiguration runtimeConfiguration;

    private Executor executor;

    @PostConstruct
    public void init() {
        executor = Executors.newFixedThreadPool(runtimeConfiguration.getAsyncSenderThread());
    }

    public void sendData(Stream stream, BusMessage busMessage, boolean async) {
        Runnable task = () -> {
            try {
                SdkBytes sdkBytes = SdkBytes.fromByteArray(SerialHelper.toString(busMessage));
                kinesis.getClient().putRecord(builder ->
                        builder.streamName(stream
                                .getDataStreamName())
                                .partitionKey(UUID.randomUUID().toString())
                                .data(sdkBytes))
                        .get();
                log.debug("Sent {} for {} into {} : {}", busMessage.getMessageType(), busMessage.getOrigin(), stream.name(),
                        busMessage.getUuid());
            } catch (IOException | InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        };
        if (async) {
            executor.execute(task);
        } else {
            task.run();
        }
    }

}
