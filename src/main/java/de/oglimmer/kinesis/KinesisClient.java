package de.oglimmer.kinesis;

import lombok.Getter;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

@Component
public class KinesisClient {

    @Getter
    private KinesisAsyncClient client = getKinesisAsyncClient();

    private KinesisAsyncClient getKinesisAsyncClient() {
        return KinesisAsyncClient.builder()
                .region(Region.EU_CENTRAL_1)
                .credentialsProvider(ProfileCredentialsProvider.create("sy"))
                .build();
    }

}
