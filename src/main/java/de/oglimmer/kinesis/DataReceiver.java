package de.oglimmer.kinesis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Slf4j
@Component
@RequiredArgsConstructor
public class DataReceiver {

    private final KinesisClient kinesis;
    private final RuntimeConfiguration runtimeConfiguration;

    private ExecutorService executorService;
    private Consumer<BusMessage> consumer;
    private Stream stream;
    private int shardNo;

    public void start(Stream stream, Consumer<BusMessage> consumer, int shardNo) {
        this.consumer = consumer;
        this.stream = stream;
        this.shardNo = shardNo;
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> init());
        log.info("Started DataReceiver for {} on shard {}", stream.name(), shardNo);
    }

    public void init() {
        try {
            List<Shard> shards = kinesis.getClient().listShards(builder -> builder.streamName(stream.getDataStreamName())).get().shards();

            GetShardIteratorRequest shardIteratorRequest = GetShardIteratorRequest.builder()
                    .streamName(stream.getDataStreamName())
                    .shardIteratorType("LATEST")
                    .shardId(shards.get(shardNo).shardId())
                    .build();

            GetShardIteratorResponse shardIteratorResponse = kinesis.getClient().getShardIterator(shardIteratorRequest).get();
            String shardIterator = shardIteratorResponse.shardIterator();
            while (true) {
                shardIterator = getRecords(shardIterator);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private String getRecords(final String shardIterator) {
        try {
            long startTime = System.currentTimeMillis();
            GetRecordsResponse recordsResponse = kinesis.getClient().getRecords(builder -> builder.shardIterator(shardIterator)).get();
            log.debug("Time to read shard: {}millis", (System.currentTimeMillis() - startTime));
            List<Record> records = recordsResponse.records();
            if (!records.isEmpty()) {
                log.debug("Found {} records in {} on shard {}", records.size(), stream.name(), shardNo);
                records.forEach(this::process);
            }

            Thread.sleep(runtimeConfiguration.getWaitTimeInBetweenRead());

            return recordsResponse.nextShardIterator();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void process(Record record) {
        try {
            consumer.accept((BusMessage) SerialHelper.fromString(record.data()));
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
