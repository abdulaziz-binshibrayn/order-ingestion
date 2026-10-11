package com.abdulaziz.orderingestion.job

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, lit, max, to_timestamp}
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}


object OrderIngestionJob {
  def main(args: Array[String]): Unit = {

    require(
      args.length ==2,
      "Expected arguments: <kafkaBootstrapServers> <warehouse>"
    )

    val kafkaBootstrapServers = args(0)
    val warehouse = args(1)


    val spark = SparkSession.builder()
      .appName("OrderIngestionJob")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config(
        "spark.sql.catalog.local.type",
        "hadoop"
      )
      .config(
        "spark.sql.catalog.local.warehouse",
        warehouse
      )
      .getOrCreate()

    val runTs = Timestamp.from(Instant.now())

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.sales")

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS local.sales.orders (
        |  orderId STRING,
        |  customerId STRING,
        |  amount DECIMAL(18, 2),
        |  currency STRING,
        |  status STRING,
        |  eventTime TIMESTAMP,
        |  kafka_ts TIMESTAMP,
        |  kafka_partition INT,
        |  kafka_offset BIGINT,
        |  run_ts TIMESTAMP
        |)
        |USING iceberg
        |""".stripMargin
    )

    val previousOffsets = spark.sql(
      """
        |SELECT summary['kafka_offsets'] AS kafka_offsets
        |FROM local.sales.orders.snapshots
        |WHERE summary['kafka_offsets'] IS NOT NULL
        |ORDER BY committed_at DESC
        |LIMIT 1
        |""".stripMargin
    ).collect()

    val startingOffsets =
      if(previousOffsets.isEmpty)
        "earliest"
      else
        previousOffsets.head.getString(0)

    val kafkaRecords = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", "orders")
      .option("startingOffsets", startingOffsets)
      .option("endingOffsets", "latest")
      .load()
      .cache()


    if(kafkaRecords.isEmpty){
      spark.stop()
      return
    }

    val records = kafkaRecords.selectExpr(
      "CAST(value AS STRING) AS value",
      "timestamp AS kafka_ts",
      "partition AS kafka_partition",
      "offset AS kafka_offset"
    )



    val offsets = kafkaRecords
      .groupBy("partition")
      .agg((max("offset") + 1).as("next_offset"))
      .collect()
      .map(row => s""""${row.getInt(0)}":${row.getLong(1)}""")
      .mkString(",")

    val nextOffsets = s"""{"orders":{$offsets}}"""

    val orderSchema = new StructType()
      .add("orderId", StringType)
      .add("customerId", StringType)
      .add("amount", DecimalType(18, 2))
      .add("currency", StringType)
      .add("status", StringType)
      .add("eventTime", StringType)
      .add("_corrupt_record", StringType)

    val parsedRecords = records.withColumn(
      "order",
      from_json(
        col("value"),
        orderSchema,
        Map(
          "mode" -> "PERMISSIVE",
          "columnNameOfCorruptRecord" -> "_corrupt_record"
        )
      )
    )

    val validRecords = parsedRecords.filter(col("order._corrupt_record").isNull)

    val finalRecords = validRecords.select(
      col("order.orderId").as("orderId"),
      col("order.customerId").as("customerId"),
      col("order.amount").as("amount"),
      col("order.currency").as("currency"),
      col("order.status").as("status"),
      to_timestamp(
        col("order.eventTime"),
        "yyyy-MM-dd'T'HH:mm:ssX"
      ).as("eventTime"),
      col("kafka_ts"),
      col("kafka_partition"),
      col("kafka_offset"),
      lit(runTs).as("run_ts")
    )



    finalRecords.writeTo("local.sales.orders")
      .option("snapshot-property.run_ts", runTs.toInstant.toString)
      .option("snapshot-property.kafka_offsets", nextOffsets)
      .append()


    spark.stop()
  }
}