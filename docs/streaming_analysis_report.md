# Streaming Pipeline Analysis Report

**Generated:** 2026-05-23 21:19:28

---

**Data Summary:** 28,130 raw trades, 147 one-minute windows, 50 five-minute windows

**Tickers tracked:** 25

**Sources:** simulator, late_data_demo

---

## 1. VWAP Summary by Ticker (5-Minute Windows)

| ticker   |   vwap |   total_volume |   trade_count |   buy_pressure |
|:---------|-------:|---------------:|--------------:|---------------:|
| NVDA     | 136.86 |        1115522 |          1884 |           54.7 |
| AMZN     | 208.05 |        1064602 |          1848 |           48.8 |
| TSLA     | 282.8  |        1048555 |          1785 |           52.4 |
| AAPL     | 209.59 |         996593 |          1823 |           52.4 |
| MSFT     | 459.94 |         965425 |          1851 |           52   |
| META     | 523.87 |         955565 |          1726 |           50.1 |
| MA       | 547.35 |         642077 |           928 |           55.2 |
| GOOGL    | 185.24 |         595925 |           906 |           48.7 |
| V        | 323.43 |         590896 |           854 |           47.8 |
| BAC      |  46.63 |         507456 |           846 |           58.7 |
| JPM      | 234.28 |         505301 |           898 |           57.4 |
| AVGO     | 186.61 |         472256 |           914 |           51.5 |
| HD       | 391.82 |         401563 |           608 |           53.4 |
| CVX      | 156.54 |         389806 |           581 |           47.4 |
| KO       |  63.49 |         383183 |           624 |           50.8 |
| MRK      | 129.99 |         370620 |           618 |           50.3 |
| ABBV     | 195.8  |         360267 |           595 |           41.3 |
| WMT      |  95.78 |         333101 |           607 |           56.5 |
| PEP      | 172.81 |         321666 |           617 |           48.4 |
| LLY      | 957.91 |         315940 |           553 |           39.7 |
| JNJ      | 167.21 |         309969 |           596 |           43.3 |
| BRK-B    | 473.5  |         307419 |           591 |           45.3 |
| PG       | 174.23 |         294791 |           569 |           54.5 |
| TMO      | 578.12 |         290979 |           562 |           36.3 |
| COST     | 938.94 |         289145 |           616 |           67.1 |
| AMZN     | 200.13 |         211174 |           322 |           39.3 |
| MSFT     | 458.57 |         210198 |           339 |           45.8 |
| META     | 526.85 |         167380 |           314 |           40.8 |
| TSLA     | 290.23 |         151109 |           284 |           58.3 |
| AAPL     | 206.78 |         125201 |           295 |           63.9 |
| NVDA     | 133.23 |         124395 |           320 |           52.5 |
| GOOGL    | 180.74 |         101937 |           164 |           34.4 |
| JPM      | 238.99 |          92417 |           154 |           68.4 |
| AVGO     | 183.24 |          85701 |           136 |           24.5 |
| COST     | 945.12 |          76373 |           101 |           38.7 |
| WMT      |  97.51 |          73397 |            99 |           42.4 |
| BAC      |  46.73 |          71363 |           159 |           79   |
| V        | 325.45 |          68905 |           163 |           41.2 |
| PG       | 174.63 |          66190 |           106 |           49.1 |
| MA       | 551.88 |          65335 |           147 |           40.3 |
| BRK-B    | 472.58 |          63975 |           114 |           80.5 |
| CVX      | 157.61 |          51461 |           107 |           53.9 |
| MRK      | 129.57 |          49950 |           105 |           45.7 |
| PEP      | 173.05 |          46604 |            94 |           61.7 |
| TMO      | 562.38 |          43247 |            96 |           78.8 |
| HD       | 390.97 |          42147 |           106 |           22.3 |
| KO       |  61.29 |          39195 |            98 |           23.8 |
| JNJ      | 166.52 |          35704 |            93 |           38.4 |
| LLY      | 952.13 |          34920 |            97 |           61.2 |
| ABBV     | 195.35 |          32847 |            87 |           24.4 |


## 2. Volume Leaders

Top 5 tickers by total volume traded:

| ticker   |   total_volume |   trade_count |   vwap |
|:---------|---------------:|--------------:|-------:|
| NVDA     |        1115522 |          1884 | 136.86 |
| AMZN     |        1064602 |          1848 | 208.05 |
| TSLA     |        1048555 |          1785 | 282.8  |
| AAPL     |         996593 |          1823 | 209.59 |
| MSFT     |         965425 |          1851 | 459.94 |


Bottom 5 tickers by volume:

| ticker   |   total_volume |   trade_count |   vwap |
|:---------|---------------:|--------------:|-------:|
| ABBV     |          32847 |            87 | 195.35 |
| LLY      |          34920 |            97 | 952.13 |
| JNJ      |          35704 |            93 | 166.52 |
| KO       |          39195 |            98 |  61.29 |
| HD       |          42147 |           106 | 390.97 |


## 3. Buy vs Sell Pressure

Buy pressure > 55% suggests bullish sentiment, < 45% suggests bearish.

**Bullish (buy pressure > 55%):**

| ticker   |   buy_pressure |
|:---------|---------------:|
| BRK-B    |           80.5 |
| BAC      |           79   |
| TMO      |           78.8 |
| JPM      |           68.4 |
| COST     |           67.1 |
| AAPL     |           63.9 |
| PEP      |           61.7 |
| LLY      |           61.2 |
| BAC      |           58.7 |
| TSLA     |           58.3 |
| JPM      |           57.4 |
| WMT      |           56.5 |
| MA       |           55.2 |


**Bearish (buy pressure < 45%):**

| ticker   |   buy_pressure |
|:---------|---------------:|
| JNJ      |           43.3 |
| WMT      |           42.4 |
| ABBV     |           41.3 |
| V        |           41.2 |
| META     |           40.8 |
| MA       |           40.3 |
| LLY      |           39.7 |
| AMZN     |           39.3 |
| COST     |           38.7 |
| JNJ      |           38.4 |
| TMO      |           36.3 |
| GOOGL    |           34.4 |
| AVGO     |           24.5 |
| ABBV     |           24.4 |
| KO       |           23.8 |
| HD       |           22.3 |


## 4. Liquidity Analysis (Bid-Ask Spread)

Tighter spread = more liquid. Spread in dollars; bps = basis points of VWAP.

**Most liquid (tightest spread):**

| ticker   |   avg_spread |   spread_bps |
|:---------|-------------:|-------------:|
| AAPL     |       0.0618 |            3 |
| AMZN     |       0.0592 |            3 |
| NVDA     |       0.0414 |            3 |
| TSLA     |       0.0859 |            3 |
| AMZN     |       0.0614 |            3 |


**Least liquid (widest spread):**

| ticker   |   avg_spread |   spread_bps |
|:---------|-------------:|-------------:|
| ABBV     |       0.1804 |          9.2 |
| JNJ      |       0.1551 |          9.3 |
| AVGO     |       0.1712 |          9.3 |
| BRK-B    |       0.4474 |          9.5 |
| COST     |       0.9584 |         10.1 |


## 5. Price Range (Volatility Proxy)

High-Low range as percentage of VWAP — wider range = more volatile.

**Most volatile:**

| ticker   |   low_price |   high_price |     vwap |   range_pct |
|:---------|------------:|-------------:|---------:|------------:|
| AMZN     |      199.01 |       222.62 | 208.055  |       11.35 |
| V        |      310.2  |       333.2  | 323.427  |        7.11 |
| KO       |       60.87 |        65.15 |  63.4916 |        6.74 |
| META     |      507.01 |       542.13 | 523.87   |        6.7  |
| NVDA     |      132.52 |       141.25 | 136.865  |        6.38 |


**Least volatile:**

| ticker   |   low_price |   high_price |     vwap |   range_pct |
|:---------|------------:|-------------:|---------:|------------:|
| KO       |       61.05 |        61.57 |  61.2907 |        0.85 |
| COST     |      941.35 |       948.77 | 945.124  |        0.79 |
| ABBV     |      194.33 |       195.88 | 195.35   |        0.79 |
| MRK      |      129.09 |       130.1  | 129.574  |        0.78 |
| LLY      |      947.87 |       954.99 | 952.133  |        0.75 |


## 6. Data Source Breakdown

| source         |   trades |   tickers |
|:---------------|---------:|----------:|
| late_data_demo |       30 |         1 |
| simulator      |    28100 |        25 |


---

*Report generated by `streaming_analysis.py` at 2026-05-23T21:19:45.387565*