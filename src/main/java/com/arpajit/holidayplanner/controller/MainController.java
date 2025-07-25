package com.arpajit.holidayplanner.controller;

import java.time.LocalDateTime;
import java.util.UUID;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import jakarta.servlet.http.HttpServletRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.arpajit.holidayplanner.dto.*;

@RestController
@RequestMapping("/holidayplanner")
public class MainController {
    private static final Logger logger = LoggerFactory.getLogger(MainController.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ControllerDataServComm dataService;

    @GetMapping("/allHolidayDetails")
    public void getAllHolidayDetails(HttpServletRequest httpRequest) throws Exception {
        logger.info("Requested {}: {}", httpRequest.getMethod(), httpRequest.getRequestURL());
        // Prepare topic message
        ProduceMessage message = new ProduceMessage(UUID.randomUUID().toString(),
                                                    "GET_ALL_HOLIDAYS",
                                                    "com.arpajit.holidayplanner.controller.getAllHolidayDetails",
                                                    LocalDateTime.now().toString(),
                                                    null,
                                                    "IN_QUEUE",
                                                    null);
        logger.info("Sending Kafka envelope: {}", objectMapper.writeValueAsString(message));
        String payload = objectMapper.writeValueAsString(message);
        // Send to Kafka
        logger.info("Sending Kafka envelope: {}", payload);
        String dataServiceResponse = dataService.addAudit(payload);
        logger.info("Response from Data Sercive on adding audit: ", dataServiceResponse);
        kafkaTemplate.send("holidayplanner-creator", payload);
        logger.info("Sent Kafka envelope: {}", payload);
    }
}
