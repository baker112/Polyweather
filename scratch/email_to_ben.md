# Email to Ben (WeatherNext team) — confirm egress is on the house

**Subject:** Quick question about egress costs on `gs://weathernext`

---

Hi Ben,

I'm a student at the University of Sheffield using the WeatherNext 2 dataset
for a personal weather forecasting project, and wanted to sanity-check
something on the GCS side before I commit to a larger backfill.

I'm reading the public zarr stores at `gs://weathernext/weathernext_2_0_0/zarr/`
from a Compute Engine VM in `northamerica-northeast2` (Toronto). The bucket
itself is in the **US multi-region**, so I'd normally expect cross-region
egress charges (~$0.02/GiB).

I ran a small test — pulled ~5 GiB of `2m_temperature` from
`2022_to_2023/predictions.zarr` — and after 24+ hours of billing propagation,
**Cloud Storage doesn't appear on my billing report at all** (no egress line
items, no transfer-out line items). I also asked on r/googlecloud and got a
reply suggesting that Google Research public datasets are configured so that
Google foots the egress bill, even though that's not the default for public
GCS buckets.

Before I kick off a full historic backfill (roughly 20 TiB across 2022–present,
pulled chunk-by-chunk over several days), I wanted to confirm directly with
your team:

1. Is `gs://weathernext/` indeed configured so that egress is covered by
   Google rather than billed to the reader's project? Or am I just seeing
   pricing that could change at scale?
2. Is there a usage scale at which I should give the team a heads-up before
   pulling — or is the bucket comfortable with multi-TiB academic reads?
3. Any reading patterns you'd prefer I avoid (e.g. concurrency limits,
   particular store layouts) to keep the bucket healthy for other users?

For context on what I'm doing: I'm wiring WeatherNext 2 as the fourth member
of a Bayesian model averaging ensemble (alongside GFS, ECMWF-OPEN, and ICON)
to predict daily-max-temperature distributions at single weather stations.
Tiny scope — one station at a time — but historically I need the full ensemble
across many init times to fit the calibration weights.

Happy to share what I build back if it's useful, and thanks again for putting
WeatherNext out as a public dataset — it's been wonderful to work with.

Best,
Oliver Baker
University of Sheffield
ohbaker1@sheffield.ac.uk

---

## Notes before sending

- **Who is Ben?** Make sure you've got the right contact — the WN2 paper's
  corresponding authors / the GCP DevRel post may list a specific person.
  If "Ben" is the Google DeepMind WeatherNext lead, the framing above is
  about right. If he's GCP-side (storage/egress engineering), drop the
  ensemble-model paragraph and keep it purely about bucket configuration.
- **Tone**: pitched as a polite student check-in rather than a corporate
  procurement question. Mentions Sheffield + .ac.uk email — gives him an
  easy "yep, academic use, no problem" mental category.
- **Risk to leak**: this email reveals you're doing forecasting but says
  nothing about trading. Safe to send as-is. If you'd rather not mention
  Polymarket at all (you don't), this version is fine.
- **What you're really asking**: "if I read 20 TiB next week will I or
  Google be on the hook for ~£400 of egress." Phrased above as a polite
  scale check — same question, gentler framing.
- **If he confirms it's free**: keep the email — it's your paper trail if
  a bill ever does show up.
- **If he says "actually it might cost you"**: don't run the backfill.
  Stick to live-only ingest (~$0.10/month at current rates) until you have
  a better plan.
