package com.arpajit.holidayplanner.controller;

import java.time.LocalDateTime;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.arpajit.holidayplanner.dto.*;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/holidayplanner")
public class HolidaysController {
    private static final Logger logger = LoggerFactory.getLogger(HolidaysController.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ControllerProducer controllerProducer;

    @Autowired
    private ObjectMapper objectMapper;

    @GetMapping("/allHolidayDetails")
    public void getAllHolidayDetails(HttpServletRequest httpRequest) throws Exception {
        logger.info("Requested {}: {}", httpRequest.getMethod(), httpRequest.getRequestURL());
        // Wrap in Kafka envelope
        controllerProducer.setRequestType("GET_ALL_HOLIDAYS");
        controllerProducer.setSourceService("controller-service");
        controllerProducer.setTimestamp(LocalDateTime.now().toString());
        // Send to Kafka
        kafkaTemplate.send("holidayplanner-creator", controllerProducer);
        logger.info("Sent Kafka envelope: {}", objectMapper.writeValueAsString(controllerProducer));
    }
}
