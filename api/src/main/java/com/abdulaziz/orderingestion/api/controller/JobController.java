package com.abdulaziz.orderingestion.api.controller;

import com.abdulaziz.orderingestion.api.service.SparkJobService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final SparkJobService sparkJobService ;

    public JobController(SparkJobService sparkJobService){
        this.sparkJobService = sparkJobService;
    }

    @PostMapping
    public ResponseEntity<Void> runJob() throws IOException, InterruptedException {

        sparkJobService.runJob();

        return  ResponseEntity.ok().build();
    }
}
