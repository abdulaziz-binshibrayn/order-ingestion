package com.abdulaziz.orderingestion.api.dto;

import java.time.Instant;

public record RunResponse(
    Instant runTs,
    long snapshotId,
    Instant commitTime,
    long rowsAdded
){}
