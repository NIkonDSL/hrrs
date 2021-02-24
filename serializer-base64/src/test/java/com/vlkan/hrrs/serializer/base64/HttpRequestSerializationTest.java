package com.vlkan.hrrs.serializer.base64;

import com.vlkan.hrrs.api.*;
import com.vlkan.hrrs.serializer.HttpRequestRecordPipe;
import com.vlkan.hrrs.serializer.base64.guava.GuavaBase64Decoder;
import com.vlkan.hrrs.serializer.base64.guava.GuavaBase64Encoder;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class HttpRequestSerializationTest {

    private static final int RANDOM_RECORD_COUNT = 100;

    private static final int MAX_GROUP_COUNT = 10;

    private static final int MAX_HEADER_COUNT = 10;

    private static final int MAX_BYTE_COUNT = 1024 * 128;      // 128 KB

    @Test
    public void should_write_and_read() throws Exception {
        Random random = new Random(0);
        for (int testIndex = 0; testIndex < RANDOM_RECORD_COUNT; testIndex++) {
            if (testIndex % 20 == 0){
                System.out.println("#"+testIndex + "/" + RANDOM_RECORD_COUNT);
            }
            HttpRequestRecord record = generateHttpRequestRecord(random);
            should_write_and_read(record);
        }
        System.err.println("ok");
    }

    private static void should_write_and_read(HttpRequestRecord record) throws Exception {
        HttpRequestRecordPipe pipe = new HttpRequestRecordPipe(MAX_BYTE_COUNT * 8);
        HttpRequestRecordReader<String> reader = new Base64HttpRequestRecordReader(pipe, GuavaBase64Decoder.getInstance());
        HttpRequestRecordWriter<String> writer = Base64HttpRequestRecordWriter.createBase64HttpRequestRecordWriter(pipe, GuavaBase64Encoder.getInstance());
        writer.write(record);

        while (!writer.isReady()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        pipe.flush();
        Iterator<HttpRequestRecord> iterator = reader.read().iterator();
        assertThat(iterator.hasNext(), is(true));
        HttpRequestRecord readRecord = iterator.next();
        assertThat(readRecord, is(equalTo(record)));
    }

    private static HttpRequestRecord generateHttpRequestRecord(Random random) {

        // Populate fields.
        String id = Integer.toString(generateInt(random, 0, Integer.MAX_VALUE), Character.MAX_RADIX);
        int groupId = Math.abs(id.hashCode()) % MAX_GROUP_COUNT;
        String groupName = String.format("group-%d", groupId);
        Date timestamp = new Date();
        String uri = String.format("/hello/%s?id=%s", groupName, id);
        HttpRequestMethod method = generateHttpRequestMethod(random);
        List<HttpRequestHeader> headers = generateHttpRequestHeaders(random);
        HttpRequestPayload payload = generateHttpRequestPayload(random);
        ResponseInfo responseInfo = new ResponseInfo(null);

        // Create the record.
        return ImmutableHttpRequestRecord
                .newBuilder()
                .setId(id)
                .setTimestamp(timestamp)
                .setGroupName(groupName)
                .setUri(uri)
                .setMethod(method)
                .setHeaders(headers)
                .setPayload(payload)
                .setResponseInfo(responseInfo)
                .build();

    }

    private static HttpRequestMethod generateHttpRequestMethod(Random random) {
        HttpRequestMethod[] values = HttpRequestMethod.values();
        int valueIndex = generateInt(random, 0, values.length);
        return values[valueIndex];
    }

    private static List<HttpRequestHeader> generateHttpRequestHeaders(Random random) {
        int headerCount = generateInt(random, 0, MAX_HEADER_COUNT);
        if (headerCount < 1) {
            return Collections.emptyList();
        }
        List<HttpRequestHeader> headers = new ArrayList<>(headerCount);
        for (int headerIndex = 0; headerIndex < headerCount; headerIndex++) {
            HttpRequestHeader header = generateHttpRequestHeader(random);
            headers.add(header);
        }
        return headers;
    }

    private static HttpRequestHeader generateHttpRequestHeader(Random random) {
        int id = Math.abs(random.nextInt());
        String name = String.format("name-%d", id);
        String value = String.format("value-%d", id);
        return ImmutableHttpRequestHeader
                .builder()
                .setName(name)
                .setValue(value)
                .build();
    }

    private static HttpRequestPayload generateHttpRequestPayload(Random random) {
        int missingByteCount = generateInt(random, 0, MAX_BYTE_COUNT);
        int byteCount = generateInt(random, 0, MAX_BYTE_COUNT);
        byte[] bytes = new byte[byteCount];
        random.nextBytes(bytes);
        return ImmutableHttpRequestPayload
                .newBuilder()
                .setMissingByteCount(missingByteCount)
                .setBytes(bytes)
                .build();
    }

    private static int generateInt(Random random, int from, int to) {
        checkArgument(from <= to, "expecting: from <= to, found: %s > %s", from, to);
        int range = to - from;
        int nextInt = random.nextInt(range);
        return from + nextInt;
    }

}
