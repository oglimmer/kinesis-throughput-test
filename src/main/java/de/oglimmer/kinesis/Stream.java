package de.oglimmer.kinesis;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum Stream {

    INBOUND("inbound"),
    OUTBOUND("outbound");

    @Getter
    private String dataStreamName;

}
