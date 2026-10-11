$container = "order-ingestion-kafka"
$topic = "orders"
$bootstrapServer = "localhost:9092"
$samplesPath = Join-Path $PSScriptRoot "..\samples"

# Create the topic if needed
docker exec $container /opt/kafka/bin/kafka-topics.sh `
    --create `
    --if-not-exists `
    --topic $topic `
    --bootstrap-server $bootstrapServer `
    --partitions 1 `
    --replication-factor 1

# Load each JSON line as a separate Kafka message
Get-ChildItem "$samplesPath\orders_batch_*.json" |
    Sort-Object Name |
    ForEach-Object {
        Write-Host "Loading $($_.Name)..."

        $containerPath = "/tmp/$($_.Name)"

        docker cp $_.FullName "${container}:$containerPath"

        docker exec $container bash -c `
            "cat '$containerPath' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server '$bootstrapServer' --topic '$topic'"

    }

Write-Host "Sample data loaded into topic '$topic'."