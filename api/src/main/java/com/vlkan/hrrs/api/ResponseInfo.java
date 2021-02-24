package com.vlkan.hrrs.api;

import javax.servlet.http.HttpServletResponse;

public class ResponseInfo {
    int statusCode;
    long responseTime;

    public ResponseInfo(HttpServletResponse response) {
        if (response == null) {
            statusCode = 200;
            responseTime = 200;
            return;
        }
        statusCode = response.getStatus();
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public void setResponseTime(long responseTime) {
        this.responseTime = responseTime;
    }

    @Override
    public String toString() {
        return "ResponseInfo{" +
                "statusCode=" + statusCode +
                ", time=" + responseTime +
                " ms}";
    }

    public long getResponseTime() {
        return responseTime;
    }
}
