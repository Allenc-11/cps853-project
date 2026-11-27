-- Part 4: empty everything the stream wrote, so a new stream run starts from nothing.
-- Batch tables and batch quality results are kept.
TRUNCATE stream_hourly, stream_hour_of_day, stream_progress, stream_rejects, injected_messages;
DELETE FROM quality_checks WHERE stage = 'stream';
DELETE FROM alerts WHERE stage = 'stream';
DELETE FROM pipeline_counts WHERE stage IN ('4 producer', '5 consumer');
