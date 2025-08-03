package com.arpajit.holidayplanner.controller;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class ControllerDataServComm {
    private static final Logger logger = LoggerFactory.getLogger(ControllerDataServComm.class);

    @Value("${dataservice.domain}")
    private String dsDomain;

    @Autowired
    private RestTemplate restTemplate;

    public String addAudit(String payload) {
        String dsUrl = dsDomain + "/add-audit";
        logger.info("Calling URL: {}", dsUrl);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(payload, headers);
        logger.info("Sending data: {}", payload);
        ResponseEntity<String> response = restTemplate.postForEntity(dsUrl, request, String.class);
        logger.info("{} : gave Status : {}", dsUrl, response.getStatusCode());
        logger.info("Received:\n{}\n",response.getBody());
        return response.getBody();
    }

    public String updateAudit(String traceId, String status, String statusResp) {
        String payload = "{\"traceId\":\""+traceId+
                            "\",\"status\":\""+status+
                            "\",\"statusResp\":\""+statusResp+"\"}";
        String dsUrl = dsDomain + "/update-audit";
        logger.info("Calling URL: {}", dsUrl);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(payload, headers);
        ResponseEntity<String> response = restTemplate.postForEntity(dsUrl, request, String.class);
        logger.info("{} : gave Status : {}", dsUrl, response.getStatusCode());
        logger.info("Received:\n{}\n",response.getBody());
        return response.getBody();
    }
}
