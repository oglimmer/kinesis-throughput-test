package de.oglimmer.kinesis;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.io.IOException;
import java.util.UUID;
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

    class DataReceiverStream {

        private Consumer<BusMessage> consumer;
        private Stream stream;

        public void start(Stream stream, Consumer<BusMessage> consumer) {
            this.consumer = consumer;
            this.stream = stream;
            init();
            log.info("Started DataReceiver for {}", stream.name());
        }

        private void init() {
            DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(kinesis.getRegion()).build();

            CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(kinesis.getRegion()).build();

            ConfigsBuilder configsBuilder = new ConfigsBuilder(
                    stream.getDataStreamName(),
                    stream.getDataStreamName(),
                    kinesis.getClient(),
                    dynamoClient,
                    cloudWatchClient,
                    UUID.randomUUID().toString(),
                    new SampleRecordProcessorFactory(consumer));

            Scheduler scheduler = new Scheduler(
                    configsBuilder.checkpointConfig(),
                    configsBuilder.coordinatorConfig(),
                    configsBuilder.leaseManagementConfig(),
                    configsBuilder.lifecycleConfig(),
                    configsBuilder.metricsConfig(),
                    configsBuilder.processorConfig(),
                    configsBuilder.retrievalConfig()
                            .retrievalSpecificConfig(new PollingConfig(stream.getDataStreamName(), kinesis.getClient()))
            );

            Thread schedulerThread = new Thread(scheduler);
            schedulerThread.setDaemon(true);
            schedulerThread.start();
        }

    }

    @AllArgsConstructor
    private static class SampleRecordProcessorFactory implements ShardRecordProcessorFactory {
        private Consumer<BusMessage> consumer;

        public ShardRecordProcessor shardRecordProcessor() {
            return new SampleRecordProcessor(consumer);
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    private static class SampleRecordProcessor implements ShardRecordProcessor {

        private static final String SHARD_ID_MDC_KEY = "ShardId";
        private final Consumer<BusMessage> consumer;
        private String shardId;

        public void initialize(InitializationInput initializationInput) {
            shardId = initializationInput.shardId();
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Initializing @ Sequence: {}", initializationInput.extendedSequenceNumber());
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }

        public void processRecords(ProcessRecordsInput processRecordsInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.debug("Processing {} record(s)", processRecordsInput.records().size());
                processRecordsInput.records().forEach(r -> {
                    log.debug("Processing record pk: {} -- Seq: {}", r.partitionKey(), r.sequenceNumber());
                    try {
                        consumer.accept((BusMessage) SerialHelper.fromString(r.data()));
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

        public void leaseLost(LeaseLostInput leaseLostInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Lost lease, so terminating.");
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }

        public void shardEnded(ShardEndedInput shardEndedInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Reached shard end checkpointing.");
                shardEndedInput.checkpointer().checkpoint();
            } catch (ShutdownException | InvalidStateException e) {
                log.error("Exception while checkpointing at shard end. Giving up.", e);
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }

        public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Scheduler is shutting down, checkpointing.");
                shutdownRequestedInput.checkpointer().checkpoint();
            } catch (ShutdownException | InvalidStateException e) {
                log.error("Exception while checkpointing at requested shutdown. Giving up.", e);
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }
    }
}
