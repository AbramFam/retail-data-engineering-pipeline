# Proposal: Product vs. Service Classification for Top Products

**Status: APPROVED (2026-10-29) and implemented as rule v1** in
[`retail_pipeline/classification.py`](../retail_pipeline/classification.py). Only the two additional
tables use it; `gold_top_products` and every KPI keep their Notebook 08 definitions.
Implementation note: the reference list is a versioned constant in code (`RULE_VERSION = "v1"`,
stored in `gold_revenue_by_class.rule_version`) rather than a separate reference table.

## Problem

`gold_top_products` ranks every `StockCode`. In the UCI data, three of the top 10 are not
merchandise: `DOT` (DOTCOM POSTAGE, #1), `M` (Manual, #8) and `POST` (POSTAGE, #9).
The original SQL has the same behaviour.

## Why a reference list, not a pattern

Merchandise codes are normally 5 digits plus up to 2 letters (`85123A`). 4,037 of 4,070 codes
follow that pattern. But the other 33 are a mix: `DCGSSBOY` (BOYS PARTY BAG), `DCGS0069`
(dog collar) and `PADS` are real products, while `POST`, `DOT` and `AMAZONFEE` are not.
A pattern alone would misclassify them, so the rule uses an explicit reference list, and
anything unrecognised **stays a product** (revenue is never hidden by default).

## Proposed rule (v1)

| Class | Codes | Rule |
|---|---|---|
| `SHIPPING` | `POST`, `DOT`, `C2` | reference list |
| `FEE_OR_CHARGE` | `AMAZONFEE`, `BANK CHARGES`, `CRUK` | reference list |
| `MANUAL_ADJUSTMENT` | `M`, `m`, `D` (Discount), `S` (Samples) | reference list |
| `ACCOUNTING_ADJUSTMENT` | `B` (bad debt; already excluded via `A...` invoices) | reference list |
| `GIFT_VOUCHER` | `gift_0001_10` ... `gift_0001_50` | pattern `^gift_\d{4}_\d+$` |
| `PRODUCT` | everything else | default |

* The list lives in one versioned place (a small reference table, `ref_stockcode_class`),
  with `rule_version`, so a reclassification is a reviewed change, not a code edit.
* Review flag: a `PRODUCT` whose code is outside `^\d{5}[A-Za-z]{0,2}$` is reported for
  review (not excluded). In UCI that is 17 codes (the `DCGS...` family and `PADS`).
* Comparison is on the trimmed code and is case-sensitive (`m` is listed explicitly).

## Where it would apply

| Output | Change if approved |
|---|---|
| GrossRevenue, ReturnsValue, NetRevenue, AOV, monthly, country | **None.** Shipping and fees are still revenue |
| `gold_top_products` | **Unchanged** (all codes) |
| New `gold_top_merchandise` | Same rules as Top Products, `PRODUCT` class only |
| New `gold_revenue_by_class` | Gross sales per class (reconciles to GrossRevenue) |

Adding new tables rather than redefining the existing one keeps the current results comparable.

## Impact on the public UCI data (local, level C)

Gross sales by class: PRODUCT 10,271,433.06 · SHIPPING 291,401.65 · MANUAL_ADJUSTMENT 78,175.87 ·
FEE_OR_CHARGE 13,926.09 · GIFT_VOUCHER 685.81. **Total 10,655,622.48 = GrossRevenue.**

| Rank | Current `gold_top_products` | Proposed `gold_top_merchandise` |
|---|---|---|
| 1 | DOT 206,248.77 | 22423 174,484.74 |
| 2 | 22423 174,484.74 | 23843 168,469.60 |
| 3 | 23843 168,469.60 | 85123A 104,518.80 |
| 4 | 85123A 104,518.80 | 47566 99,504.33 |
| 5 | 47566 99,504.33 | 85099B 94,340.05 |
| 6 | 85099B 94,340.05 | 23166 81,700.92 |
| 7 | 23166 81,700.92 | 23084 66,964.99 |
| 8 | M 78,110.27 | 22086 64,952.29 |
| 9 | POST 78,101.88 | 84879 59,094.93 |
| 10 | 23084 66,964.99 | 79321 54,117.76 |

## Separate open question (not covered by this rule)

`23843` (PAPER CRAFT, LITTLE BIRDIE) ranks #2–3 from one 80,995-unit order that was cancelled.
Top Products uses gross sales by definition. Whether to rank on net-of-cancellation sales
is a different decision and is not part of this proposal.

## Decisions (approved 2026-10-29)

1. Approve the classes and the reference list above (or edit them).
2. Approve adding `gold_top_merchandise` and `gold_revenue_by_class` (existing tables unchanged).
3. Confirm that unknown codes default to `PRODUCT` with a review flag.
