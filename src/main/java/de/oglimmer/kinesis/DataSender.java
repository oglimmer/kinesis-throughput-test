package de.oglimmer.kinesis;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Component
public class DataSender {

    private final KinesisClient kinesis;

    private KinesisProducer kinesisProducer;

    {
        KinesisProducerConfiguration kinesisProducerConfiguration = new KinesisProducerConfiguration();
        kinesisProducerConfiguration.setCredentialsProvider(new ProfileCredentialsProvider("sy"));
        kinesisProducerConfiguration.setRegion("eu-central-1");

        kinesisProducer = new KinesisProducer(kinesisProducerConfiguration);
    }

    @SneakyThrows
    public void sendData(Stream stream, BusMessage busMessage) {
        final UserRecord userRecord = new UserRecord();
        userRecord.setStreamName(stream.getDataStreamName());
        userRecord.setPartitionKey(UUID.randomUUID().toString());
        try {
            userRecord.setData(ByteBuffer.wrap(SerialHelper.toString(busMessage)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        final ListenableFuture<UserRecordResult> userRecordResultListenableFuture = kinesisProducer.addUserRecord(userRecord);
        log.debug("Sent {} for {} into {} : {} to SHARD: {}", busMessage.getMessageType(), busMessage.getOrigin(),
                stream.name(), busMessage.getUuid(), userRecordResultListenableFuture.get().getShardId());
    }

}
