package de.oglimmer.kinesis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
@RequiredArgsConstructor
@Slf4j
public class DataInputHandler {

    private final DataReceiver dataReceiver;
    private final AsyncDataSender asyncDataSender;
    private final RuntimeStatistics runtimeStatistics;
    private final ReqRespVerifier reqRespVerifier;

    public void start(Stream listenStream, Stream replyStream, Origin noResponseFor) {
        DataInputHandlerStream dataInputHandlerStream = new DataInputHandlerStream();
        dataInputHandlerStream.start(listenStream, replyStream, noResponseFor);
    }

    class DataInputHandlerStream implements Consumer<BusMessage> {

        private Stream listenStream;
        private Stream replyStream;
        private Origin noResponseFor;

        public void start(Stream listenStream, Stream replyStream, Origin noResponseFor) {
            if (listenStream == replyStream) {
                throw new IllegalArgumentException();
            }
            this.listenStream = listenStream;
            this.replyStream = replyStream;
            this.noResponseFor = noResponseFor;
            dataReceiver.start(listenStream, this);
        }

        @Override
        public void accept(BusMessage busMessage) {
            if (busMessage.getOrigin() != noResponseFor) {
                log.debug("Received (and will reply) from {} : {}", listenStream.name(), busMessage);
                asyncDataSender.sendData(replyStream, new BusMessage(busMessage));
                runtimeStatistics.getStreamStatsMap().get(listenStream).incRecordAnswere();
            } else {
                reqRespVerifier.unregister(busMessage);
            }
        }

    }

}
