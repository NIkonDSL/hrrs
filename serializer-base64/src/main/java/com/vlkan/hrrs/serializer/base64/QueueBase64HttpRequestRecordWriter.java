package com.vlkan.hrrs.serializer.base64;

import com.vlkan.hrrs.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.vlkan.hrrs.serializer.base64.Base64HttpRequestRecord.*;

public class QueueBase64HttpRequestRecordWriter extends Base64HttpRequestRecordWriter implements HttpRequestRecordWriter<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueBase64HttpRequestRecordWriter.class);
    private static final String ENCODED_DATA_PLACEHOLDER = "<!--ENCODED_DATA-->";
    private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss.SSSZ");
    private static volatile boolean stop = false;
    private static CountDownLatch stopped = new CountDownLatch(1);
    private static CountDownLatch started = new CountDownLatch(1);
    private long lastFlush = System.currentTimeMillis();
    private static volatile DoWithRecord doWithRecord = null;
    private static final int CAPACITY = 1000;
    private static BlockingQueue<ToBeRecorded> queue = new ArrayBlockingQueue<>(CAPACITY);

    private static class ToBeRecorded {
        String content;
        byte[] binaryData;

        public ToBeRecorded(String content) {
            this.content = content;
        }
    }

    private interface DoWithRecord {
        void apply(ToBeRecorded record) throws Exception;
    }

    private static Thread writer = new Thread(() -> {
        started.countDown();
        while (!stop || !queue.isEmpty()) {
            try {
                ToBeRecorded toBeRecorded = queue.poll(50, TimeUnit.MILLISECONDS);
                if (toBeRecorded == null) {
                    continue;
                }
                if (toBeRecorded.content.isEmpty()) {
                    //stop signal detected
                    LOGGER.info("Stop signal detected");
                    break;
                }
                try {
                    doWithRecord.apply(toBeRecorded);
                } catch (Exception error) {
                    String message = String.format("record serialization failure (id=%s)", toBeRecorded);
                    throw new RuntimeException(message, error);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.info("Interruption detected");
                break;
            }
        }
        LOGGER.info("Writer is stopped");
        stopped.countDown();
    });
    static {
        writer.setName("HRRS Writer");
        if (!stop) {
            LOGGER.info("Writer is ready");
            writer.start();
        }
    }

    public QueueBase64HttpRequestRecordWriter(HttpRequestRecordWriterTarget<String> target, Base64Encoder encoder) {
        super(target, encoder);
        //stop signal detected
        QueueBase64HttpRequestRecordWriter.doWithRecord = record -> {
            String content = record.content;
            String encodedRecordBytes = encoder.encode(record.binaryData);
            content = content.replace(ENCODED_DATA_PLACEHOLDER, encodedRecordBytes);
            target.write(content);
            if (queue.isEmpty() || (System.currentTimeMillis() - lastFlush > 10000)) {
                target.flush();
            }
        };
    }

    @Override
    public HttpRequestRecordWriterTarget<String> getTarget() {
        return target;
    }

    @Override
    public void write(HttpRequestRecord record) throws IOException {
        try {
            started.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Cannot wait till Writer is started");
            throw new IllegalStateException();
        }
        //get out of sync code
        String formattedDate = dateFormat.format(record.getTimestamp());
        StringBuilder toBeRecorded = new StringBuilder(512);
        byte[] recordBytes = writeRecord(record);

        toBeRecorded
                .append(record.getId())
                .append(FIELD_SEPARATOR)
                .append(formattedDate)
                .append(FIELD_SEPARATOR)
                .append(record.getGroupName())
                .append(FIELD_SEPARATOR)
                .append(record.getMethod().toString())
                .append(FIELD_SEPARATOR)
                .append(ENCODED_DATA_PLACEHOLDER)
                .append(FIELD_SEPARATOR)
                .append(record.getResponseInfo().getStatusCode())
                .append(FIELD_SEPARATOR)
                .append(record.getResponseInfo().getResponseTime())
                .append(RECORD_SEPARATOR);
        ToBeRecorded toRecord = new ToBeRecorded(toBeRecorded.toString());
        toRecord.binaryData = recordBytes;
        if (!queue.offer(toRecord)) {
            LOGGER.warn("Queue is overflowed. Skipping writing to file...");
        }
    }

    private byte[] writeRecord(HttpRequestRecord record) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        writeRecord(record, dataOutputStream);
        dataOutputStream.flush();
        return outputStream.toByteArray();
    }

    private static void writeRecord(HttpRequestRecord record, DataOutputStream stream) throws IOException {
        stream.writeUTF(record.getUri());
        writeHeaders(record.getHeaders(), stream);
        writePayload(record.getPayload(), stream);
    }

    private static void writeHeaders(List<HttpRequestHeader> headers, DataOutputStream stream) throws IOException {
        int headerCount = headers.size();
        stream.writeInt(headerCount);
        for (HttpRequestHeader header : headers) {
            stream.writeUTF(header.getName());
            stream.writeUTF(header.getValue());
        }
    }

    private static void writePayload(HttpRequestPayload payload, DataOutputStream stream) throws IOException {
        stream.writeInt(payload.getMissingByteCount());
        stream.writeInt(payload.getBytes().length);
        stream.write(payload.getBytes());
    }

    public boolean isReady() throws InterruptedException {
        started.await();
        return queue.isEmpty();
    }

    @Override
    public void close() throws InterruptedException {
        LOGGER.info("Stopping...");
        stop = true;
        queue.put(new ToBeRecorded(""));
        stopped.await();
    }

    public static String getStat() {
        int size = queue.size();
        return "Writer queue: " + size + "/" + CAPACITY + " (" +
                (int) (size * 100.0 / CAPACITY)
                + "%)";
    }
}
