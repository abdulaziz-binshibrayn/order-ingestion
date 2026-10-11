package com.abdulaziz.orderingestion.api.controller;

import com.abdulaziz.orderingestion.api.dto.RunResponse;
import com.abdulaziz.orderingestion.api.service.IcebergRunService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/runs")
public class RunController {

    private final IcebergRunService icebergRunService;

    public RunController(IcebergRunService icebergRunService){
        this.icebergRunService = icebergRunService;
    }

    @GetMapping
    public List<RunResponse> getRuns() throws IOException{
        return icebergRunService.getRuns();
    }

    @PostMapping("/rollback/{snapshotId}")
    public ResponseEntity<Void> rollback(@PathVariable long snapshotId) throws IOException{

        icebergRunService.rollback(snapshotId);

        return ResponseEntity.noContent().build();
    }


}
