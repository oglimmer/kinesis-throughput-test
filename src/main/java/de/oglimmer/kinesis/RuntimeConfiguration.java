package de.oglimmer.kinesis;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
@Getter
@Setter
@Slf4j
public class RuntimeConfiguration {

    @Value("${mode}")
    private Mode mode;

    @Value("${dataGenerationSpeed}")
    private long dataGenerationSpeed;

    @Value("${payloadSize}")
    private int payloadSize;

    @Value("${dataGenerationThread}")
    private int dataGenerationThread;

    @Value("${asyncSenderThread}")
    private int asyncSenderThread;


    @PostConstruct
    public void printValues() {
        log.info("\r\nMode: {}\r\ndataGenerationSpeed: {}" +
                        "\r\npayloadSize: {}\r\ndataGenerationThread: {}\r\nasyncSenderThread: {}", mode,
                dataGenerationSpeed, payloadSize, dataGenerationThread, asyncSenderThread);
    }

}
