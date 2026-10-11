# Singapore Air Quality Prometheus Exporter

A Prometheus exporter for NEA's air quality datasets as a Cloudflare Worker.

## Datasets

- [Pollutant Standards Index (PSI)](https://data.gov.sg/datasets/d_fe37906a0182569d891506e815e819b7/view)
- [Hourly PM2.5](https://data.gov.sg/datasets/d_e1058d6974c877257e32048ab128ad83/view)

The worker polls the datasets every 15 minutes, storing the readings in Workers KV.
In the event of a failed scrape, readings are considered current for 60 minutes. However, the `nea_polly_success` metric will be `0`.

## Metrics

Scrape `/metrics` for these gauges:

| Metric | Labels |
| --- | --- |
| `nea_psi_24_hour` | `region` |
| `nea_pm25_1_hour_micrograms_per_cubic_meter` | `region` |
| `nea_poll_success` | `dataset` |
| `nea_last_successful_poll_timestamp_seconds` | `dataset` |
| `nea_observation_timestamp_seconds` | `dataset` |
