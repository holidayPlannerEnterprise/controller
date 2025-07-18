package com.arpajit.holidayplanner.controller;

import org.slf4j.*;
import org.springframework.web.bind.annotation.*;
import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/holidayplanner")
public class HolidaysController {
    private static final Logger logger = LoggerFactory.getLogger(HolidaysController.class);

    @GetMapping("/allHolidayDetails")
    public void getAllHolidayDetails(HttpServletRequest httpRequest) throws Exception {
        logger.info("Requested {}: {}", httpRequest.getMethod(), httpRequest.getRequestURL());
    }
}
