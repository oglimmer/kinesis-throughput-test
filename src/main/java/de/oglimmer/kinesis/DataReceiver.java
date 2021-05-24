package de.oglimmer.kinesis;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncStrategyType;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.function.Consumer;


@Slf4j
@Component
@RequiredArgsConstructor
public class DataReceiver {

    private final KinesisClient kinesis;

    public void start(Stream stream, Consumer<BusMessage> consumer) {
        DataReceiverStream dataReceiverStream = new DataReceiverStream();
        dataReceiverStream.start(stream, consumer);
    }

    @AllArgsConstructor
    private static class SampleRecordProcessorFactory implements IRecordProcessorFactory {
        private Consumer<BusMessage> consumer;

        @Override
        public IRecordProcessor createProcessor() {
            return new SampleRecordProcessor(consumer);
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    private static class SampleRecordProcessor implements IRecordProcessor {

        private static final String SHARD_ID_MDC_KEY = "ShardId";
        private final Consumer<BusMessage> consumer;
        private String shardId;

        public void initialize(InitializationInput initializationInput) {
            shardId = initializationInput.getShardId();
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Initializing for {} @ Sequence: {}", shardId, initializationInput.getExtendedSequenceNumber());
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }

        @Override
        public void processRecords(ProcessRecordsInput processRecordsInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.debug("Processing {} record(s) on {}", processRecordsInput.getRecords().size(), shardId);
                processRecordsInput.getRecords().forEach(r -> {
                    log.debug("Processing record pk: {} -- Seq: {}", r.getPartitionKey(), r.getSequenceNumber());
                    try {
                        consumer.accept((BusMessage) SerialHelper.fromString(r.getData()));
                    } catch (IOException | ClassNotFoundException e) {
                        log.error("Failed to consume record", e);
                    }
                });
            } catch (Throwable t) {
                log.error("Caught throwable while processing records. Aborting.");
                Runtime.getRuntime().halt(1);
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }

        @Override
        public void shutdown(ShutdownInput shutdownInput) {
            try {
                shutdownInput.getCheckpointer().checkpoint();
            } catch (InvalidStateException e) {
                e.printStackTrace();
            } catch (ShutdownException e) {
                e.printStackTrace();
            }
        }

    }

    class DataReceiverStream {

        private Consumer<BusMessage> consumer;
        private Stream stream;

        public void start(Stream stream, Consumer<BusMessage> consumer) {
            this.consumer = consumer;
            this.stream = stream;
            Executors.newSingleThreadExecutor().submit(() -> init());
        }

        private void init() {
            log.info("Started DataReceiver for {}", stream.name());
            try {
                final KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(
                        stream.getDataStreamName(),
                        stream.getDataStreamName(),
                        new ProfileCredentialsProvider("sy"),
                        InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID()
                );
                config.withRegionName("eu-central-1");
//                config.withShardSyncStrategyType(ShardSyncStrategyType.PERIODIC);
                final IRecordProcessorFactory recordProcessorFactory = new SampleRecordProcessorFactory(consumer);
                {
                    final Worker worker = new Worker.Builder()
                            .recordProcessorFactory(recordProcessorFactory)
                            .config(config)
                            .build();
                    try {
                        worker.run();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }

    }
}
