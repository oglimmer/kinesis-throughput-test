package de.oglimmer.kinesis;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Arrays;

@Configuration
@Getter
@Slf4j
public class RuntimeConfiguration {

    @Value("${mode}")
    private Mode mode;

    @Value("${shardsToRead}")
    private String shardsToRead;

    @Value("${dataGenerationSpeed}")
    private long dataGenerationSpeed;

    @Value("${waitTimeInBetweenRead}")
    private long waitTimeInBetweenRead;

    @Value("${payloadSize}")
    private int payloadSize;

    @Value("${dataGenerationThread}")
    private int dataGenerationThread;

    @Value("${asyncSenderThread}")
    private int asyncSenderThread;

    public int[] getShardsToReadArray() {
        return Arrays.stream(shardsToRead.split(",")).mapToInt(Integer::parseInt).toArray();
    }

    @PostConstruct
    public void printValues() {
        log.info("\r\nMode: {}\r\nshardsToRead: {}\r\ndataGenerationSpeed: {}\r\nwaitTimeInBetweenRead: " +
                        "{}\r\npayloadSize: {}\r\ndataGenerationThread: {}\r\nasyncSenderThread: {}", mode,
                shardsToRead, dataGenerationSpeed, waitTimeInBetweenRead, payloadSize, dataGenerationThread, asyncSenderThread);
    }

}
