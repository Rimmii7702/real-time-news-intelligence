# Recorded Kibana screenshots and provenance

These four images were copied **byte-for-byte** from a prior supplied CS 6350 streaming project archive. They represent a historical NER-only run; they are **not** output from the newly integrated AG News classification stage. No new images were synthesized and no chart values were edited.

| Observation | Original UTC capture timestamp | File |
|---|---|---|
| ~15 minutes | 2026-11-12 20:38:12.681435 | `screenshots/kibana_top10_15min.png` |
| ~30 minutes | 2026-11-12 20:53:12.830442 | `screenshots/kibana_top10_30min.png` |
| ~45 minutes | 2026-11-12 21:08:12.796161 | `screenshots/kibana_top10_45min.png` |
| ~60 minutes | 2026-11-12 21:23:13.235564 | `screenshots/kibana_top10_60min.png` |

The **30- and 45-minute screenshots are byte-identical** (`sha256=afb3e030f38b8179f1d8bf0d557cbc676dfadca70f9b1b0a2209408cddaa135e`). The 30- and 45-minute `top10` CSV exports are also equal. This should be interpreted as no change in that displayed top-ten view, not as proof that the entire feed was static.

The saved prior-run audit at `results/archive_2026-11-12_pipeline_verification.json` reports 69 unique articles, 208 entity mentions, 141 indexed `(entity,type)` documents, 695 entity update records, and three Boolean consistency checks marked true. These are *recorded observations*, not independently rerun here.

After a fresh end-to-end run, capture a new set of browser screenshots separately, with capture times and the data view/filter settings used. Do not silently replace archived evidence or mix old and new results in the same metric claim.
