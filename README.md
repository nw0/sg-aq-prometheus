# Singapore Air Quality Prometheus Exporter

A Prometheus exporter for NEA's air quality datasets as a Cloudflare Worker.

## Datasets

- [Pollutant Standards Index (PSI)](https://data.gov.sg/datasets/d_fe37906a0182569d891506e815e819b7/view)
- [Hourly PM2.5](https://data.gov.sg/datasets/d_e1058d6974c877257e32048ab128ad83/view)

The worker polls the datasets every 15 minutes, storing the readings in Workers KV.
In the event of a failed scrape, readings are considered current for 60 minutes. However, the `nea_poll_success` metric will be `0`.

## Prometheus configuration

I've hosted this on a public worker.
See <https://sg-aq.nicholas-593.workers.dev/metrics>.

Add this job to `scrape_configs` in your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: singapore_air_quality
    scheme: https
    metrics_path: /metrics
    scrape_interval: 5m
    static_configs:
      - targets:
          - sg-aq.nicholas-593.workers.dev
```

## Metrics

Scrape `/metrics` for these gauges:

| Metric | Labels | Meaning |
| --- | --- | --- |
| `nea_psi_24_hour` | `region`, `dataset="psi"` | Overall 24-hour Pollutant Standards Index (PSI), a dimensionless index determined by the highest pollutant sub-index. |
| `nea_pm10_24_hour_micrograms_per_cubic_meter` | `region`, `dataset="psi"` | PM10 concentration averaged over the preceding 24 hours, in µg/m³. PM10 refers to particulate matter up to 10 micrometres in diameter. |
| `nea_pm25_24_hour_micrograms_per_cubic_meter` | `region`, `dataset="psi"` | PM2.5 concentration averaged over the preceding 24 hours, in µg/m³. PM2.5 refers to fine particulate matter up to 2.5 micrometres in diameter. |
| `nea_pm10_sub_index` | `region`, `dataset="psi"` | Dimensionless PSI sub-index derived from the 24-hour PM10 concentration. |
| `nea_pm25_sub_index` | `region`, `dataset="psi"` | Dimensionless PSI sub-index derived from the 24-hour PM2.5 concentration. |
| `nea_no2_1_hour_max_micrograms_per_cubic_meter` | `region`, `dataset="psi"` | Maximum 1-hour nitrogen dioxide (NO₂) concentration, in µg/m³. |
| `nea_so2_24_hour_micrograms_per_cubic_meter` | `region`, `dataset="psi"` | Sulphur dioxide (SO₂) concentration averaged over the preceding 24 hours, in µg/m³. |
| `nea_co_8_hour_max_milligrams_per_cubic_meter` | `region`, `dataset="psi"` | Maximum 8-hour carbon monoxide (CO) concentration, in mg/m³. |
| `nea_o3_8_hour_max_micrograms_per_cubic_meter` | `region`, `dataset="psi"` | Maximum 8-hour ozone (O₃) concentration, in µg/m³. |
| `nea_so2_sub_index` | `region`, `dataset="psi"` | Dimensionless PSI sub-index derived from the SO₂ concentration. |
| `nea_co_sub_index` | `region`, `dataset="psi"` | Dimensionless PSI sub-index derived from the CO concentration. |
| `nea_o3_sub_index` | `region`, `dataset="psi"` | Dimensionless PSI sub-index derived from the ozone concentration. |
| `nea_pm25_1_hour_micrograms_per_cubic_meter` | `region`, `dataset="pm25"` | PM2.5 concentration averaged over the preceding hour, in µg/m³. |
| `nea_poll_success` | `dataset` | `1` if the latest poll of the dataset succeeded; `0` if it failed or has not yet succeeded. |
| `nea_last_successful_poll_timestamp_seconds` | `dataset` | Unix timestamp (seconds since the UTC epoch) of the last successful poll. Absent until a poll succeeds. |
| `nea_observation_timestamp_seconds` | `dataset` | Unix timestamp of the retained dataset observation, rather than the time the exporter fetched it. Absent until a valid observation is stored. |

The `dataset` label identifies the source: `psi` is the PSI dataset (including
PM10, PM2.5, NO₂, SO₂, CO, and ozone concentrations and the supplied pollutant
sub-indices), and `pm25` is the
hourly PM2.5 dataset. The `region` label identifies the region supplied by NEA:
`north`, `south`, `east`, `west`, `central`, or `national`, where available.
Missing or invalid readings are omitted rather than reported as zero.

The 24-hour concentrations are rolling averages, updated hourly by NEA.
CO concentrations use mg/m³; the other concentrations use µg/m³.
The exporter uses NEA's supplied sub-indices; see
[NEA's PSI computation guide](https://www.haze.gov.sg/docs/default-source/faq/computation-of-the-pollutant-standards-index-%28psi%29.pdf)
for how pollutant concentrations map to PSI values.
