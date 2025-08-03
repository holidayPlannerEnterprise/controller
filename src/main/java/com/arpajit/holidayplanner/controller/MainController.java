package com.arpajit.holidayplanner.controller;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.web.bind.annotation.*;
import jakarta.servlet.http.HttpServletRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.arpajit.holidayplanner.controller.dto.KafkaMessage;

@RestController
@RequestMapping("/holidayplanner")
public class MainController {
    private static final Logger logger = LoggerFactory.getLogger(MainController.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate;

    @Autowired
    private ControllerDataServComm dataService;

    @GetMapping("/allHolidayDetails")
    public ResponseEntity<String> getAllHolidayDetails(HttpServletRequest httpRequest) throws Exception {
        logger.info("Requested {}: {}", httpRequest.getMethod(), httpRequest.getRequestURL());

        // Prepare topic message
        KafkaMessage messageDTO = new KafkaMessage(UUID.randomUUID().toString(),
                                                    "GET_ALL_HOLIDAY_DETAILS",
                                                    "com.arpajit.holidayplanner.controller.getAllHolidayDetails",
                                                    LocalDateTime.now().toString(),
                                                    null,
                                                    "CREATOR_SENT",
                                                    null);
        String message = objectMapper.writeValueAsString(messageDTO);

        // Preparing Kafka payload
        ProducerRecord<String, String> payload = new ProducerRecord<>("holidayplanner-creator", message);

        // Sending to Creator topic
        logger.info("Sending Kafka message: {}", message);
        RequestReplyFuture<String, String, String> responsePayload = replyingKafkaTemplate.sendAndReceive(payload);
        dataService.addAudit(message);
        logger.info("Dropped successfully into Creator topic");

        // Comsuming Sync Response from Controller topic
        ConsumerRecord<String, String> response = responsePayload.get(30, TimeUnit.SECONDS);
        logger.info("Successfully received data from Dispatcher topic: \n{}", response.value());
        messageDTO = objectMapper.readValue(response.value(), KafkaMessage.class);
        messageDTO.setStatus("CONTROLLER_RECEIVED");
        dataService.updateAudit(messageDTO.getTraceId(),
                                messageDTO.getStatus(),
                                messageDTO.getStatusResp());
        return ResponseEntity.ok(messageDTO.getPayload());
    }
}
