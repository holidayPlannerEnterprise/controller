package com.arpajit.holidayplanner.controller.dto;

import java.time.LocalDateTime;

public class ExceptionResponse {
    private String errorCode;
    private String errorMsg;
    private LocalDateTime timeStamp;

    public ExceptionResponse(String errorCode, String errorMsg) {
        this.errorCode = errorCode;
        this.errorMsg = errorMsg;
        this.timeStamp = LocalDateTime.now();
    }

    public String getErrorCode() {return errorCode;}
    public String getErrorMsg() {return errorMsg;}
    public String getTimeStamp() {return timeStamp.toString();}
}
