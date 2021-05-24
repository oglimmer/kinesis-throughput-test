package de.oglimmer.kinesis;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import lombok.Getter;
import org.springframework.stereotype.Component;

@Component
public class KinesisClient {

    @Getter
    private AmazonKinesis client = getKinesisAsyncClient();

    private AmazonKinesis getKinesisAsyncClient() {
        return AmazonKinesisClient.builder().withRegion(Regions.EU_CENTRAL_1)
                .withCredentials(new ProfileCredentialsProvider("sy"))
                .build();
    }

}
