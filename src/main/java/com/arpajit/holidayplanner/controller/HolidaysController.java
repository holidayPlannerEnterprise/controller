package com.arpajit.holidayplanner.controller;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import jakarta.servlet.http.HttpServletRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.arpajit.holidayplanner.dto.*;

@RestController
@RequestMapping("/holidayplanner")
public class HolidaysController {
    private static final Logger logger = LoggerFactory.getLogger(HolidaysController.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @GetMapping("/allHolidayDetails")
    public void getAllHolidayDetails(HttpServletRequest httpRequest) throws Exception {
        logger.info("Requested {}: {}", httpRequest.getMethod(), httpRequest.getRequestURL());
        // Prepare topic message
        ProduceMessage message = new ProduceMessage("GET_ALL_HOLIDAYS",
                                                "com.arpajit.holidayplanner.controller.getAllHolidayDetails",
                                                LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")),
                                                null);
        String payload = objectMapper.writeValueAsString(message);
        // Send to Kafka
        kafkaTemplate.send("holidayplanner-creator", payload);
        logger.info("Sent Kafka envelope: {}", payload);
    }
}
