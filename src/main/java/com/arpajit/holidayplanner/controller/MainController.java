package com.arpajit.holidayplanner.controller;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
// import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
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

    // @Autowired
    // private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate;

    @Autowired
    private ControllerDataServComm dataService;

    @GetMapping("/allHolidayDetails")
    public ResponseEntity<String> getAllHolidayDetails(HttpServletRequest httpRequest) throws Exception {
        logger.info("Requested {}: {}", httpRequest.getMethod(), httpRequest.getRequestURL());
        // Prepare topic message
        ProduceMessage message = new ProduceMessage(UUID.randomUUID().toString(),
                                                    "GET_ALL_HOLIDAYS",
                                                    "com.arpajit.holidayplanner.controller.getAllHolidayDetails",
                                                    LocalDateTime.now().toString(),
                                                    null,
                                                    "IN_QUEUE",
                                                    null);
        String payload = objectMapper.writeValueAsString(message);
        // Send to Kafka
        String dataServiceResponse = dataService.addAudit(payload);
        logger.info("Response from Data Sercive on adding audit: ", dataServiceResponse);
        ProducerRecord<String, String> record = new ProducerRecord<>("holidayplanner-creator", payload);
        logger.info("Sending Kafka envelope: {}", payload);
        // kafkaTemplate.send("holidayplanner-creator", payload);
        RequestReplyFuture<String, String, String> responsePayload = replyingKafkaTemplate.sendAndReceive(record);
        logger.info("Sent Kafka envelope: {}", payload);
        ConsumerRecord<String, String> response = responsePayload.get(10, TimeUnit.SECONDS);
        dataService.updateAudit(message.getTraceId(), "SUCCESS", null);
        return ResponseEntity.ok(response.value());
    }
}
