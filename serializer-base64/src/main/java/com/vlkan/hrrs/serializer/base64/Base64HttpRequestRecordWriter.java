package com.vlkan.hrrs.serializer.base64;

import com.vlkan.hrrs.api.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.vlkan.hrrs.serializer.base64.Base64HttpRequestRecord.*;

public class Base64HttpRequestRecordWriter implements HttpRequestRecordWriter<String> {

    protected final Base64Encoder encoder;
    protected final HttpRequestRecordWriterTarget<String> target;
    private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss.SSSZ");

    protected Base64HttpRequestRecordWriter(HttpRequestRecordWriterTarget<String> target, Base64Encoder encoder) {
        this.target = checkNotNull(target, "target");
        this.encoder = checkNotNull(encoder, "encoder");
    }

    public static Base64HttpRequestRecordWriter createBase64HttpRequestRecordWriter(HttpRequestRecordWriterTarget<String> target, Base64Encoder encoder) {
        if (System.getProperty("use.hrrs.vanilla") != null) {
            return new Base64HttpRequestRecordWriter(target, encoder);
        }
        return new QueueBase64HttpRequestRecordWriter(target, encoder);
    }

    @Override
    public HttpRequestRecordWriterTarget<String> getTarget() {
        return target;
    }

    @Override
    public void write(HttpRequestRecord record) throws IOException {
        //get out of sync code
        try {
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
            synchronized (this) {
                target.write(toBeRecorded.toString());
            }
        } catch (Exception error) {
            String message = String.format("record serialization failure (id=%s)", record.getId());
            throw new RuntimeException(message, error);
        }
    }

    @Override
    public void close() throws InterruptedException {

    }

    @Override
    public boolean isReady() throws InterruptedException {
        return true;
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

}
