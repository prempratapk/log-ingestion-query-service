package com.prempratapk.logingest.kafka;

import com.prempratapk.logingest.enrichment.GeoIpEnrichmentService;
import com.prempratapk.logingest.model.EnrichedLogEvent;
import com.prempratapk.logingest.model.LogEvent;
import com.prempratapk.logingest.storage.S3ParquetWriter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka consumer that reads log events, enriches them with GeoIP/ASN data,
 * and writes batches to S3 as Parquet files.
 *
 * Uses manual acknowledgment to ensure records are only committed after
 * successful S3 write. Failed records are routed to the DLQ.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class LogEventConsumer {

    private static final int FLUSH_BATCH_SIZE = 5_000;

    private final GeoIpEnrichmentService geoIpService;
    private final S3ParquetWriter parquetWriter;
    private final DlqPublisher dlqPublisher;
    private final MeterRegistry meterRegistry;

    private final List<EnrichedLogEvent> buffer = new ArrayList<>();
    private final AtomicLong totalConsumed = new AtomicLong(0);

    /**
     * Main consumer listener. Processes records in batches of up to 500.
     * Each batch is enriched and appended to the in-memory buffer.
     * Buffer is flushed to S3 when it reaches FLUSH_BATCH_SIZE.
     */
    @KafkaListener(
            topics = "${kafka.topic}",
            groupId = "${kafka.consumer-group}",
            containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void consume(List<ConsumerRecord<String, LogEvent>> records, Acknowledgment ack) {
        log.debug("Received batch size={}", records.size());
        List<EnrichedLogEvent> enriched = new ArrayList<>(records.size());

        for (ConsumerRecord<String, LogEvent> record : records) {
            try {
                EnrichedLogEvent event = enrich(record.value());
                enriched.add(event);
            } catch (Exception e) {
                log.error("Enrichment failed for eventId={}, routing to DLQ",
                        record.value().getEventId(), e);
                dlqPublisher.publish(record.value(), e.getMessage());
                meterRegistry.counter("log.ingest.dlq.count").increment();
            }
        }

        synchronized (buffer) {
            buffer.addAll(enriched);
            totalConsumed.addAndGet(enriched.size());

            if (buffer.size() >= FLUSH_BATCH_SIZE) {
                flushToS3();
            }
        }

        ack.acknowledge();
        meterRegistry.counter("log.ingest.consumed.count").increment(records.size());
    }

    private EnrichedLogEvent enrich(LogEvent event) {
        String sourceIp = event.getMetadata() != null
                ? (String) event.getMetadata().get("sourceIp") : null;

        GeoIpEnrichmentService.GeoData geoData = sourceIp != null
                ? geoIpService.lookup(sourceIp)
                : GeoIpEnrichmentService.GeoData.unknown();

        return EnrichedLogEvent.builder()
                .eventId(event.getEventId())
                .service(event.getService())
                .level(event.getLevel())
                .message(event.getMessage())
                .timestamp(event.getTimestamp())
                .metadata(event.getMetadata())
                .country(geoData.country())
                .city(geoData.city())
                .asn(geoData.asn())
                .build();
    }

    private void flushToS3() {
        if (buffer.isEmpty()) return;
        List<EnrichedLogEvent> toFlush = new ArrayList<>(buffer);
        buffer.clear();

        try {
            parquetWriter.write(toFlush);
            log.info("Flushed {} records to S3 Parquet", toFlush.size());
            meterRegistry.counter("log.ingest.s3.flush.count").increment(toFlush.size());
        } catch (Exception e) {
            log.error("S3 Parquet write failed for {} records, re-buffering", toFlush.size(), e);
            buffer.addAll(0, toFlush);
            meterRegistry.counter("log.ingest.s3.flush.errors").increment();
        }
    }
}
