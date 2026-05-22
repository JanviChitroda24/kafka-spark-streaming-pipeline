# Delta Table Verification Report

**Generated:** 2026-05-22 19:22:20

---

## raw_trades

**Path:** `/Users/janvichitroda/Documents/Janvi/NEU/Kaizen/Data_Engineering_Portfolio/kafka-spark-streaming-pipeline/data/delta/raw_trades`

**Row count:** 28,130

**Columns:** trade_id, ticker, price, quantity, side, trade_type, exchange, source, event_time, kafka_ingest_time, dollar_volume

**Sample rows:**

| trade_id                             | ticker   |   price |   quantity | side   | trade_type   | exchange   | source    | event_time                 | kafka_ingest_time          |   dollar_volume |
|:-------------------------------------|:---------|--------:|-----------:|:-------|:-------------|:-----------|:----------|:---------------------------|:---------------------------|----------------:|
| 2a880943-04ba-4ee0-80c4-a11fbca2f1fe | AVGO     |  184.75 |         88 | BUY    | odd_lot      | NASDAQ     | simulator | 2026-05-21 07:12:10.747946 | 2026-05-21 07:12:10.748000 |        16258    |
| 5bb94b65-d5c8-47ce-90b8-f84b1ed777be | PG       |  173.26 |         44 | SELL   | odd_lot      | NYSE       | simulator | 2026-05-21 07:12:10.748536 | 2026-05-21 07:12:10.748000 |         7623.44 |
| 68d3033c-b384-4b98-818f-2d0bc9dfedc6 | MRK      |  128.73 |        500 | SELL   | round_lot    | ARCA       | simulator | 2026-05-21 07:12:10.748589 | 2026-05-21 07:12:10.748000 |        64365    |
| 13c861e0-6954-4582-b718-d953a6d82a4a | JPM      |  232.7  |        500 | BUY    | round_lot    | ARCA       | simulator | 2026-05-21 07:12:10.748760 | 2026-05-21 07:12:10.748000 |       116350    |
| 8c4012e4-8837-476a-8440-45127a279086 | AMZN     |  212.57 |        100 | SELL   | round_lot    | NASDAQ     | simulator | 2026-05-21 07:12:10.748829 | 2026-05-21 07:12:10.748000 |        21257    |


**Top 5 tickers by trade count:**

| ticker   |   count |
|:---------|--------:|
| NVDA     |    2204 |
| MSFT     |    2190 |
| AMZN     |    2170 |
| AAPL     |    2148 |
| TSLA     |    2069 |


**Events by source:**

| source         |   count |
|:---------------|--------:|
| simulator      |   28100 |
| late_data_demo |      30 |

---

## vwap_1min

**Path:** `/Users/janvichitroda/Documents/Janvi/NEU/Kaizen/Data_Engineering_Portfolio/kafka-spark-streaming-pipeline/data/delta/vwap_1min`

**Row count:** 147

**Columns:** ticker, vwap, low_price, high_price, avg_price, total_volume, total_dollar_volume, trade_count, buy_volume, avg_bid, avg_ask, sell_volume, buy_pressure, avg_spread, window_start, window_end, window_duration

**Sample rows:**

| ticker   |     vwap |   low_price |   high_price |   avg_price |   total_volume |   total_dollar_volume |   trade_count |   buy_volume |   avg_bid |   avg_ask |   sell_volume |   buy_pressure |   avg_spread | window_start        | window_end          | window_duration   |
|:---------|---------:|------------:|-------------:|------------:|---------------:|----------------------:|--------------:|-------------:|----------:|----------:|--------------:|---------------:|-------------:|:--------------------|:--------------------|:------------------|
| MRK      | 130.03   |      130.03 |       130.03 |    130.03   |            800 |              104024   |             2 |          500 |  129.97   |   130.09  |           300 |          62.5  |       0.12   | 2026-05-21 07:10:00 | 2026-05-21 07:11:00 | 1 minute          |
| GOOGL    | 186.407  |      186.27 |       186.41 |    186.343  |           5117 |              953846   |             3 |         5000 |  186.22   |   186.467 |           117 |          97.71 |       0.2467 | 2026-05-21 07:10:00 | 2026-05-21 07:11:00 | 1 minute          |
| TMO      | 575.798  |      575.79 |       575.8  |    575.795  |            600 |              345479   |             2 |            0 |  575.535  |   576.055 |           600 |           0    |       0.52   | 2026-05-21 07:10:00 | 2026-05-21 07:11:00 | 1 minute          |
| CVX      | 154.489  |      154.46 |       154.7  |    154.607  |            577 |               89140.2 |             3 |          500 |  154.537  |   154.677 |            77 |          86.66 |       0.14   | 2026-05-21 07:10:00 | 2026-05-21 07:11:00 | 1 minute          |
| BAC      |  45.4027 |       45.33 |        45.52 |     45.4233 |           1632 |               74097.2 |             6 |         1532 |   45.4017 |    45.445 |           100 |          93.87 |       0.0433 | 2026-05-21 07:10:00 | 2026-05-21 07:11:00 | 1 minute          |


**VWAP statistics:**

| summary   |     vwap |   total_volume |   trade_count |   buy_pressure |
|:----------|---------:|---------------:|--------------:|---------------:|
| min       |  45.4027 |             86 |         1     |         0      |
| max       | 963.105  |         326458 |       496     |       100      |
| mean      | 308.947  |         108570 |       191.156 |        49.8878 |

---

## vwap_5min

**Path:** `/Users/janvichitroda/Documents/Janvi/NEU/Kaizen/Data_Engineering_Portfolio/kafka-spark-streaming-pipeline/data/delta/vwap_5min`

**Row count:** 50

**Columns:** ticker, vwap, low_price, high_price, avg_price, total_volume, total_dollar_volume, trade_count, buy_volume, avg_bid, avg_ask, sell_volume, buy_pressure, avg_spread, window_start, window_end, window_duration

**Sample rows:**

| ticker   |    vwap |   low_price |   high_price |   avg_price |   total_volume |   total_dollar_volume |   trade_count |   buy_volume |   avg_bid |   avg_ask |   sell_volume |   buy_pressure |   avg_spread | window_start        | window_end          | window_duration   |
|:---------|--------:|------------:|-------------:|------------:|---------------:|----------------------:|--------------:|-------------:|----------:|----------:|--------------:|---------------:|-------------:|:--------------------|:--------------------|:------------------|
| NVDA     | 136.865 |      132.52 |       141.25 |     136.702 |        1115522 |           1.52675e+08 |          1884 |       610040 |  136.681  |  136.723  |        505482 |          54.69 |       0.0414 | 2026-05-21 07:10:00 | 2026-05-21 07:15:00 | 5 minutes         |
| MSFT     | 459.937 |      447.58 |       475.51 |     460.827 |         965425 |           4.44034e+08 |          1851 |       501828 |  460.757  |  460.897  |        463597 |          51.98 |       0.1397 | 2026-05-21 07:10:00 | 2026-05-21 07:15:00 | 5 minutes         |
| BAC      |  46.631 |       45.31 |        47.62 |      46.581 |         507456 |           2.36632e+07 |           846 |       297687 |   46.5603 |   46.6017 |        209769 |          58.66 |       0.0413 | 2026-05-21 07:10:00 | 2026-05-21 07:15:00 | 5 minutes         |
| MA       | 547.35  |      532.08 |       556.43 |     547.189 |         642077 |           3.51441e+08 |           928 |       354149 |  546.939  |  547.439  |        287928 |          55.16 |       0.4998 | 2026-05-21 07:10:00 | 2026-05-21 07:15:00 | 5 minutes         |
| BRK-B    | 473.502 |      465.51 |       480.56 |     473.291 |         307419 |           1.45564e+08 |           591 |       139340 |  473.076  |  473.506  |        168079 |          45.33 |       0.4301 | 2026-05-21 07:10:00 | 2026-05-21 07:15:00 | 5 minutes         |


**VWAP statistics:**

| summary   |    vwap |     total_volume |   trade_count |   buy_pressure |
|:----------|--------:|-----------------:|--------------:|---------------:|
| min       |  46.631 |  32847           |            87 |        22.31   |
| max       | 957.912 |      1.11552e+06 |          1884 |        80.48   |
| mean      | 313.247 | 319195           |           562 |        49.4602 |

---

## anomaly_alerts

**Path:** `/Users/janvichitroda/Documents/Janvi/NEU/Kaizen/Data_Engineering_Portfolio/kafka-spark-streaming-pipeline/data/delta/anomaly_alerts`

**Row count:** 125

**Columns:** ticker, window_avg_price, window_volume, window_trade_count, window_start, window_end, anomaly_upper, anomaly_lower, threshold_pct

**Sample rows:**

| ticker   |   window_avg_price |   window_volume |   window_trade_count | window_start        | window_end          |   anomaly_upper |   anomaly_lower |   threshold_pct |
|:---------|-------------------:|----------------:|---------------------:|:--------------------|:--------------------|----------------:|----------------:|----------------:|
| V        |           325.498  |           68905 |                  163 | 2026-05-21 07:15:00 | 2026-05-21 07:16:00 |        332.008  |        318.988  |               2 |
| NVDA     |           133.373  |          124395 |                  320 | 2026-05-21 07:15:00 | 2026-05-21 07:16:00 |        136.04   |        130.705  |               2 |
| TMO      |           562.155  |           43247 |                   96 | 2026-05-21 07:15:00 | 2026-05-21 07:16:00 |        573.398  |        550.912  |               2 |
| WMT      |            97.5161 |           73397 |                   99 | 2026-05-21 07:15:00 | 2026-05-21 07:16:00 |         99.4664 |         95.5657 |               2 |
| PG       |           174.193  |           66190 |                  106 | 2026-05-21 07:15:00 | 2026-05-21 07:16:00 |        177.677  |        170.709  |               2 |


**Unique tickers monitored:** 25

**Threshold:** 2.0%

---

## Summary

| Table | Rows | Status |
|-------|------|--------|
| raw_trades | 28,130 | ✅ OK |
| vwap_1min | 147 | ✅ OK |
| vwap_5min | 50 | ✅ OK |
| anomaly_alerts | 125 | ✅ OK |

*Report generated by `verify_delta.py` at 2026-05-22T19:22:51.214518*