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
    private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss.SSSZ");
    private static volatile boolean stop = false;
    private static CountDownLatch stopped = new CountDownLatch(1);
    private static CountDownLatch started = new CountDownLatch(1);
    private long lastFlush = System.currentTimeMillis();
    private static volatile DoWithRecord doWiRecord = null;
    private static BlockingQueue<String> queue = new ArrayBlockingQueue<>(1000);

    private interface DoWithRecord {
        void apply(String record) throws Exception;
    }

    private static Thread writer = new Thread(() -> {
        started.countDown();
        while (!stop || !queue.isEmpty()) {
            try {
                String toBeRecorded = queue.poll(50, TimeUnit.MILLISECONDS);
                if (toBeRecorded == null) {
                    continue;
                }
                if (toBeRecorded.isEmpty()) {
                    //stop signal detected
                    LOGGER.info("Stop signal detected");
                    break;
                }
                try {
                    doWiRecord.apply(toBeRecorded);
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
        QueueBase64HttpRequestRecordWriter.doWiRecord = record -> {
            target.write(record);
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
        String encodedRecordBytes = encoder.encode(recordBytes);
        toBeRecorded
                .append(record.getId())
                .append(FIELD_SEPARATOR)
                .append(formattedDate)
                .append(FIELD_SEPARATOR)
                .append(record.getGroupName())
                .append(FIELD_SEPARATOR)
                .append(record.getMethod().toString())
                .append(FIELD_SEPARATOR)
                .append(encodedRecordBytes)
                .append(FIELD_SEPARATOR)
                .append(record.getResponseInfo().getStatusCode())
                .append(FIELD_SEPARATOR)
                .append(record.getResponseInfo().getResponseTime())
                .append(RECORD_SEPARATOR);
        if (!queue.offer(toBeRecorded.toString())) {
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
        queue.put("");
        stopped.await();
    }
}
