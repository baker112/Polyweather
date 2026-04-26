# Phase 0 — Polymarket London Temperature Resolution Forensics

**Status:** INCOMPLETE — must be filled in before any model code is trusted.

---

## Step 1: Identify the resolving ICAO station

**Claim to verify:** Polymarket resolves against **EGLL** (London Heathrow).

- Inspect past resolved markets at: `https://polymarket.com/markets?category=weather`
- Look for "highest temperature in London" resolved markets and read the resolution source.
- **Alternatively:** check Polymarket's resolution FAQ or the specific market description field for the data source.

| Hypothesis | Evidence |
|------------|----------|
| EGLL (Heathrow) | [FILL IN] |
| EGLC (City) | [FILL IN] |
| Other | [FILL IN] |

**Confirmed station:** [ TO BE FILLED ]

---

## Step 2: Exact data field used

Options to verify:
- Computed daily max from hourly METAR 2m temperature readings (Iowa Mesonet ASOS)
- NWS-equivalent TMAX from daily climate summaries (SYNOP / CLIMAT)
- Highest 6-hourly SYNOP observation

| Field | Source | Notes |
|-------|--------|-------|
| Hourly METAR 2m max | Iowa Mesonet ASOS | `tmpf` field, max over local calendar day |
| TMAX climate summary | Met Office / NOAA GHCN | May lag by 1-2 days |
| 6-hourly SYNOP max | ECMWF ERA5 reanalysis | Less timely |

**Confirmed field:** [ TO BE FILLED ] — update `resolution_field` in `config/stations.yaml`

---

## Step 3: Calendar day and timezone

- Is the daily max computed over UTC day (00z–23:59z)?
- Or local time (00:00 BST/GMT – 23:59 BST/GMT)?

London is **UTC+0** (GMT) in winter, **UTC+1** (BST) from last Sunday in March to last Sunday in October.

**Confirmed timezone rule:** [ TO BE FILLED ]

---

## Step 4: Tiebreak / missing data rule

If no data is available for the calendar day:
- Does the market resolve NO CONTEST / void?
- Is there a fallback data source?
- What happens if the station reports a data gap?

**Confirmed tiebreak:** [ TO BE FILLED ]

---

## Step 5: Source URL / official rule

Copy the exact text from Polymarket's resolution criteria:

```
[PASTE VERBATIM RESOLUTION RULE HERE]
```

Source URL: [ TO BE FILLED ]

---

## Step 6: 30-outcome verification

Pull the last 30 resolved London temperature markets from Polymarket's resolved-markets API:

```
GET https://gamma-api.polymarket.com/markets?category=weather&closed=true&tag=temperature&limit=50
```

For each, reconstruct the `daily_max_c` from METAR and compare:

| Market date | Market resolving bracket | Our reconstructed max (°C) | Match? |
|-------------|-------------------------|---------------------------|--------|
| 2025-06-01  | [FILL]                  | [FILL]                    | [ ]    |
| 2025-06-02  | [FILL]                  | [FILL]                    | [ ]    |
| 2025-06-03  | [FILL]                  | [FILL]                    | [ ]    |
| ...         | ...                     | ...                       | ...    |

**All 30 must match exactly before proceeding to model code.**

If any mismatch: stop and investigate. Common causes:
- Wrong ICAO station
- UTC vs local calendar day mismatch
- Data field difference (METAR 2m vs screen thermometer)

---

## Status checklist

- [ ] Resolving ICAO station confirmed
- [ ] Exact data field confirmed  
- [ ] Timezone rule confirmed
- [ ] Tiebreak rule documented
- [ ] Source URL documented
- [ ] 30/30 historical outcomes match

**Do not proceed to Stage 1 (ingest_forecasts) until all boxes are checked.**
