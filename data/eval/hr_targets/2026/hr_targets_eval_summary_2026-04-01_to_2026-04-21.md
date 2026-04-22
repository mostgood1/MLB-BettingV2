# HR Target Eval Range Recap — 2026-04-01 to 2026-04-21

- season: 2026
- source json: data/eval/hr_targets/2026/hr_targets_eval_summary_2026-04-01_to_2026-04-21.json
- days found: 21 / 21

## Selected overall

- settled_rows: 162 | wins: 35 | losses: 127 | hit_rate: 0.216 | avg_p: 0.1525 | avg_support: 80.08 | brier: 0.1744 | logloss: 0.5404

## Excluded overall

- settled_rows: 5652 | wins: 493 | losses: 5159 | hit_rate: 0.0872 | avg_p: 0.0532 | avg_support: 58.63 | brier: 0.0802 | logloss: 0.4903

## Excluded examples overall

- settled_rows: 489 | wins: 59 | losses: 430 | hit_rate: 0.1207 | avg_p: 0.0926 | avg_support: 72.37 | brier: 0.1079 | logloss: 0.3799

## Selected Probability Buckets

| bucket | n | wins | hit_rate | avg_p |
|---|---|---|---|---|
| [0.10, 0.15) | 88 | 20 | 0.2273 | 0.133 |
| [0.15+ ] | 74 | 15 | 0.2027 | 0.175 |

## Selected Support Buckets

| bucket | n | wins | hit_rate | avg_support |
|---|---|---|---|---|
| [60.00, 70.00) | 13 | 3 | 0.2308 | 65.08 |
| [70.00+ ] | 149 | 32 | 0.2148 | 81.39 |

## Selected Signal Summary

- settled_rows: 162 | pearson_support_vs_success: 0.1485 | spearman_support_vs_success: 0.1765 | pearson_prob_vs_success: -0.0184 | pearson_score_vs_success: 0.048

## Selected Rank Method Comparison

- coverage: selected_rows_only_daily_top_n

### Score Order

| top_n | settled_rows | wins | hit_rate | avg_p | avg_support |
|---|---|---|---|---|---|
| 3 | 57 | 15 | 0.2632 | 0.1729 | 81.67 |
| 5 | 93 | 22 | 0.2366 | 0.164 | 81.22 |
| 10 | 152 | 35 | 0.2303 | 0.154 | 80.28 |

### Probability Order

| top_n | settled_rows | wins | hit_rate | avg_p | avg_support |
|---|---|---|---|---|---|
| 3 | 57 | 15 | 0.2632 | 0.1741 | 80.07 |
| 5 | 93 | 22 | 0.2366 | 0.1647 | 80.2 |
| 10 | 152 | 33 | 0.2171 | 0.1543 | 80.11 |

## Excluded Reason Breakdown

| reason | settled_rows | wins | hit_rate | avg_p | avg_support |
|---|---|---|---|---|---|
| below_min_prob | 4484 | 447 | 0.0997 | 0.0669 | 63.9 |
| prediction_ineligible | 1165 | 46 | 0.0395 |  | 38.37 |
| below_support_score | 3 | 0 |  | 0.1737 | 51.67 |

## Daily Breakdown

| date | found | sel_n | sel_wins | sel_hit_rate | excl_n | excl_wins | excl_coverage |
|---|---|---|---|---|---|---|---|
| 2026-04-01 | yes | 12 | 2 | 0.1667 | 293 | 27 | full |
| 2026-04-02 | yes | 0 | 0 |  | 89 | 11 | full |
| 2026-04-03 | yes | 12 | 3 | 0.25 | 298 | 27 | full |
| 2026-04-04 | yes | 12 | 2 | 0.1667 | 328 | 21 | full |
| 2026-04-05 | yes | 9 | 1 | 0.1111 | 332 | 30 | full |
| 2026-04-06 | yes | 5 | 2 | 0.4 | 256 | 25 | full |
| 2026-04-07 | yes | 12 | 2 | 0.1667 | 317 | 15 | full |
| 2026-04-08 | yes | 4 | 0 |  | 295 | 11 | full |
| 2026-04-09 | yes | 0 | 0 |  | 131 | 5 | full |
| 2026-04-10 | yes | 4 | 1 | 0.25 | 295 | 30 | full |
| 2026-04-11 | yes | 7 | 1 | 0.1429 | 304 | 30 | full |
| 2026-04-12 | yes | 9 | 2 | 0.2222 | 291 | 31 | full |
| 2026-04-13 | yes | 10 | 2 | 0.2 | 194 | 26 | full |
| 2026-04-14 | yes | 9 | 1 | 0.1111 | 306 | 30 | full |
| 2026-04-15 | yes | 10 | 1 | 0.1 | 300 | 34 | full |
| 2026-04-16 | yes | 9 | 2 | 0.2222 | 203 | 14 | full |
| 2026-04-17 | yes | 6 | 1 | 0.1667 | 304 | 29 | full |
| 2026-04-18 | yes | 7 | 2 | 0.2857 | 311 | 31 | full |
| 2026-04-19 | yes | 12 | 5 | 0.4167 | 318 | 23 | full |
| 2026-04-20 | yes | 6 | 2 | 0.3333 | 183 | 17 | full |
| 2026-04-21 | yes | 7 | 3 | 0.4286 | 304 | 26 | full |

