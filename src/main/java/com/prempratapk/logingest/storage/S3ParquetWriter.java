package com.prempratapk.logingest.storage;

import com.prempratapk.logingest.model.EnrichedLogEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Writes batches of enriched log events to Amazon S3 as Parquet files.
 *
 * Files are partitioned by year/month/day/hour to enable efficient
 * Athena partition pruning. Snappy compression reduces storage cost
 * by ~55% vs raw JSON. KMS server-side encryption is applied on upload.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class S3ParquetWriter {

    private static final Schema SCHEMA = SchemaBuilder.record("EnrichedLogEvent")
            .namespace("com.prempratapk.logingest")
            .fields()
            .requiredString("event_id")
            .requiredString("service")
            .requiredString("level")
            .requiredString("message")
            .requiredLong("timestamp_epoch_ms")
            .optionalString("country")
            .optionalString("city")
            .optionalString("asn")
            .optionalString("source_ip")
            .endRecord();

    private final S3Client s3Client;

    @Value("${s3.bucket}")
    private String bucket;

    @Value("${s3.prefix:logs/}")
    private String prefix;

    @Value("${aws.kms-key-arn:}")
    private String kmsKeyArn;

    /**
     * Writes a batch of enriched events to a single Parquet file on S3.
     * The S3 key is partitioned by the timestamp of the first record in the batch.
     * Uses a local temp file as buffer before uploading.
     */
    public void write(List<EnrichedLogEvent> events) throws IOException {
        if (events.isEmpty()) return;

        File tempFile = File.createTempFile("log-events-", ".parquet");
        try {
            writeParquetToFile(events, tempFile);
            String s3Key = buildS3Key(events.get(0).getTimestamp());
            uploadToS3(tempFile, s3Key);
            log.info("Uploaded Parquet file key={} records={} sizeBytes={}",
                    s3Key, events.size(), tempFile.length());
        } finally {
            Files.deleteIfExists(tempFile.toPath());
        }
    }

    private void writeParquetToFile(List<EnrichedLogEvent> events, File file) throws IOException {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.toURI());
        Configuration conf = new Configuration();

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(128L * 1024 * 1024)  // 128MB row groups
                .withPageSize(1024 * 1024)              // 1MB pages
                .withConf(conf)
                .build()) {

            for (EnrichedLogEvent event : events) {
                GenericRecord record = new GenericData.Record(SCHEMA);
                record.put("event_id", event.getEventId());
                record.put("service", event.getService());
                record.put("level", event.getLevel());
                record.put("message", event.getMessage());
                record.put("timestamp_epoch_ms", event.getTimestamp().toEpochMilli());
                record.put("country", event.getCountry());
                record.put("city", event.getCity());
                record.put("asn", event.getAsn());
                record.put("source_ip",
                        event.getMetadata() != null ? (String) event.getMetadata().get("sourceIp") : null);
                writer.write(record);
            }
        }
    }

    private void uploadToS3(File file, String key) throws IOException {
        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/octet-stream");

        if (kmsKeyArn != null && !kmsKeyArn.isBlank()) {
            requestBuilder
                    .serverSideEncryption(ServerSideEncryption.AWS_KMS)
                    .ssekmsKeyId(kmsKeyArn);
        }

        s3Client.putObject(requestBuilder.build(), RequestBody.fromFile(file));
    }

    /**
     * Builds the S3 key with Hive-style partitioning for Athena compatibility.
     * Example: logs/year=2026/month=03/day=11/hour=08/events-abc123.parquet
     */
    private String buildS3Key(Instant timestamp) {
        ZonedDateTime zdt = timestamp.atZone(ZoneOffset.UTC);
        return String.format("%syear=%d/month=%02d/day=%02d/hour=%02d/events-%s.parquet",
                prefix,
                zdt.getYear(),
                zdt.getMonthValue(),
                zdt.getDayOfMonth(),
                zdt.getHour(),
                UUID.randomUUID().toString().substring(0, 8));
    }
}
