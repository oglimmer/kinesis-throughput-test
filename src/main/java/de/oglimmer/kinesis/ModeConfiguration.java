package de.oglimmer.kinesis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class ModeConfiguration {

    private final DataInputHandler dataInputHandler;
    private final DataGenerator dataGenerator;
    private final RuntimeConfiguration runtimeConfiguration;
    private final RuntimeStatistics runtimeStatistics;

    @PostConstruct
    public void init() {
        log.info("Using mode {}", runtimeConfiguration.getMode());
        runtimeStatistics.init();
        if (runtimeConfiguration.getMode() == Mode.TRANSCEIVER) {
            dataGenerator.init(Stream.INBOUND, Origin.TRANSCEIVER);
            dataInputHandler.start(Stream.OUTBOUND, Stream.INBOUND, Origin.TRANSCEIVER);
        } else if (runtimeConfiguration.getMode() == Mode.MESSAGEHANDLER) {
//            dataGenerator.start(Stream.OUTBOUND, Origin.MESSAGEHANDLER);
            dataInputHandler.start(Stream.INBOUND, Stream.OUTBOUND, Origin.MESSAGEHANDLER);
        } else {
            log.warn("Illegal mode {} !!!", runtimeConfiguration.getMode());
        }
    }

}
