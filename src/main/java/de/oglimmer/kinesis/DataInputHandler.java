package de.oglimmer.kinesis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
@RequiredArgsConstructor
@Slf4j
public class DataInputHandler implements Consumer<BusMessage> {

    private final DataReceiver dataReceiver;
    private final DataSender dataSender;
    private final RuntimeStatistics runtimeStatistics;
    private final ReqRespVerifier reqRespVerifier;

    private Stream listenStream;
    private Stream replyStream;
    private Origin noResponseFor;

    public void start(Stream listenStream, Stream replyStream, int shartNo, Origin noResponseFor) {
        if (listenStream == replyStream) {
            throw new IllegalArgumentException();
        }
        this.listenStream = listenStream;
        this.replyStream = replyStream;
        this.noResponseFor = noResponseFor;
        dataReceiver.start(listenStream, this, shartNo);
    }

    @Override
    public void accept(BusMessage busMessage) {
        log.debug("Received from {} : {}", listenStream.name(), busMessage.toString());
        if (busMessage.getOrigin() != noResponseFor) {
            dataSender.sendData(replyStream, new BusMessage(busMessage), true);
            runtimeStatistics.getStreamStatsMap().get(listenStream).incRecordAnswere();
        } else {
            reqRespVerifier.unregister(busMessage);
        }
    }

}
