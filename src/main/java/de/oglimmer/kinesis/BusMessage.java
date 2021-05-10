package de.oglimmer.kinesis;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.time.Instant;

@Getter
@Setter
@ToString(exclude = "payload")
public class BusMessage implements Serializable {

    private static long counter = 0;

    private MessageType messageType;
    private Origin origin;
    private String uuid;
    private Instant creationTime;
    private String payload;

    public BusMessage(Origin origin, String payload) {
        this.messageType = MessageType.REQUEST;
        this.origin = origin;
        this.uuid = Long.toString(counter++);
        this.creationTime = Instant.now();
        this.payload = payload;
    }

    public BusMessage(BusMessage reply) {
        this.messageType = MessageType.RESPONSE;
        this.origin = reply.getOrigin();
        this.uuid = reply.getUuid();
        this.creationTime = Instant.now();
        this.payload = "Reply to : " + reply.getPayload();
    }

}
