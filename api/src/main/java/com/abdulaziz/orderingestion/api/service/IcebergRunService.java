package com.abdulaziz.orderingestion.api.service;

import com.abdulaziz.orderingestion.api.dto.RunResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Service
public class IcebergRunService {

    private static final TableIdentifier ORDERS_TABLE = TableIdentifier.of("sales", "orders");

    private final String warehouse;

    public IcebergRunService(
            @Value("${iceberg.warehouse}") String warehouse){
        this.warehouse = warehouse;
    }

    public List<RunResponse> getRuns() throws IOException {

        String warehousePath = Path.of(warehouse)
                .toAbsolutePath()
                .normalize()
                .toUri()
                .toString();

        try (
                HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehousePath)) {

            if (!catalog.tableExists(ORDERS_TABLE)) {
                return List.of();
            }

            Table table = catalog.loadTable(ORDERS_TABLE);

            List<RunResponse> runs = new ArrayList<>();

            for(Snapshot snapshot : table.snapshots()){

                String runTs = snapshot.summary().get("run_ts");

                if(runTs == null){
                    continue;
                }

                long rowsAdded = Long.parseLong(
                        snapshot.summary().getOrDefault(
                                SnapshotSummary.ADDED_RECORDS_PROP,
                                "0"
                        )
                );

                runs.add(new RunResponse(
                        Instant.parse(runTs),
                        snapshot.snapshotId(),
                        Instant.ofEpochMilli(snapshot.timestampMillis()),
                        rowsAdded
                ));
            }

            runs.sort(Comparator.comparing(RunResponse::commitTime));

            return runs;

        }
    }

    public void rollback(long snapshotId) throws IOException{

        String warehousePath = Path.of(warehouse)
                .toAbsolutePath()
                .normalize()
                .toUri()
                .toString();

        try(HadoopCatalog catalog = new HadoopCatalog(new Configuration(),warehousePath)){

            if(!catalog.tableExists(ORDERS_TABLE)){
                throw new IllegalStateException("Table not exist");
            }

            Table table = catalog.loadTable(ORDERS_TABLE);

            if(table.snapshot(snapshotId) == null){
                throw new IllegalArgumentException("Snapshot not found: " + snapshotId);
            }

            table.manageSnapshots().rollbackTo(snapshotId).commit();
        }

    }
}
