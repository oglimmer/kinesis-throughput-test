package de.oglimmer.kinesis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
@RequiredArgsConstructor
@Component
public class AsyncDataSender {

    private final RuntimeConfiguration runtimeConfiguration;
    private final DataSender dataSender;

    private Executor executor;

    @PostConstruct
    public void init() {
        executor = Executors.newFixedThreadPool(runtimeConfiguration.getAsyncSenderThread());
    }

    public void sendData(Stream stream, BusMessage busMessage) {
        executor.execute(() -> dataSender.sendData(stream, busMessage));
    }

}
