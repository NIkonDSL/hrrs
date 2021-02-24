package com.vlkan.hrrs.api;

import java.io.IOException;

public interface HttpRequestRecordWriter<T> {

    HttpRequestRecordWriterTarget<T> getTarget();

    void write(HttpRequestRecord record) throws IOException;

    void close() throws InterruptedException;

    boolean isReady() throws InterruptedException;
}
