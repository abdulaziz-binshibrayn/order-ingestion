package com.abdulaziz.orderingestion.api.service;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class SparkJobService {

    private static final String MAIN_CLASS = "com.abdulaziz.orderingestion.job.OrderIngestionJob";

    private final String sparkSubmit;

    private final String sparkJobJar;

    private final String kafkaBootstrapServers;

    private final String icebergWarehouse;

    public SparkJobService(
            @Value("${spark.submit}") String sparkSubmit,
            @Value("${spark.job.jar}") String sparkJobJar,
            @Value("${kafka.bootstrap-servers}") String kafkaBootstrapServers,
            @Value("${iceberg.warehouse}") String icebergWarehouse){

        this.sparkSubmit = sparkSubmit;
        this.sparkJobJar = sparkJobJar;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.icebergWarehouse = icebergWarehouse;
    }


    public void runJob() throws IOException, InterruptedException {
        Process process = new ProcessBuilder(
                sparkSubmit,
                "--master", "local[*]",
                "--class", MAIN_CLASS,
                sparkJobJar,
                kafkaBootstrapServers,
                icebergWarehouse
        )
                .inheritIO()
                .start();

        int exit = process.waitFor();

        if (exit != 0) {
            throw new IllegalStateException(
                    "spark job failed " + exit
            );

        }
    }
}
