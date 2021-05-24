package de.oglimmer.kinesis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class SerialHelper {

    public static byte[] toString(Serializable o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            return baos.toByteArray();
        }
    }

    public static Object fromString(ByteBuffer byteBuffer) throws IOException, ClassNotFoundException {
        byte[] array = new byte[byteBuffer.remaining()];
        byteBuffer.get(array);
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(array))) {
            return ois.readObject();
        }
    }
}
