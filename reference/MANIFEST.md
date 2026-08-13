# Extract manifest

One row per committed parquet. See `README.md` for the extraction recipe and the
monthly-loop caveat.

The **extracted** date is not decoration: APDF is restated over time, so the same month pulled
twice will differ. A benchmark result is reproducible only against the extract it was computed
from.

| File | R call (incl. non-default args) | Extracted | Rows |
|---|---|---|---|
| apdf_202406.parquet | apdf_tidy(wef="2024-06-01", til="2024-07-01") | 2026-08-05 | 1,161,115 |
| flights_202406.parquet | flights_tidy(wef="2024-06-01", til="2024-07-01") | 2026-08-05 | 935,887 |
| apdf_202506.parquet | apdf_tidy(wef="2025-06-01", til="2025-07-01") | 2026-08-05 | 1,224,742 |
| flights_202506.parquet | flights_tidy(wef="2025-06-01", til="2025-07-01") | 2026-08-05 | 957,396 |
| apdf_full_202506.parquet | extract_apdf_full.R (2025-06-01 -> 2025-07-01) | 2026-08-13 | 1,224,742 |
| apdf_full_202406.parquet | extract_apdf_full.R (2024-06-01 -> 2024-07-01) | 2026-08-13 | 1,161,115 |