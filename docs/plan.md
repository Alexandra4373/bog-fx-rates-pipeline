# Pipeline plan

## Source

- What: daily Bank of Ghana interbank cedi exchange rates (buying, selling, mid) for 19 currency pairs
- Where: StatsGH Data API, https://www.statsgh.com/api/data/bog-fx-rates (original source: Bank of Ghana, https://www.bog.gov.gh/treasury-and-the-markets/daily-interbank-fx-rates/)
- Access: no key needed, limit 60 requests/minute per IP
- Format: one JSON object with metadata (title, source, licence, last_updated, row_count, columns) and a "rows" list with one object per record
- Terms: CC BY 4.0. Credit "StatsGH" and the Bank of Ghana in the README and in any output

## Record key

- One record = one currency pair on one business day
- Key: rate_date + pair
- Checked: 361 rows, 19 pairs x 19 days, no duplicates

## Observations from the raw data

- Fields: rate_date (text, YYYY-MM-DD), pair (text), buying, selling, mid (numbers), source_url (text)
- Rows are ordered by date, then pair, but the code must not rely on that. Sort explicitly
- History so far: 2026-10-23 to 2026-11-18, with no missing business days
- The data runs 1-2 business days behind (on 2026-11-20 the latest date was 2026-11-18)
- mid equals the average of buying and selling in all 361 rows (within 0.0001)
- Five pairs are quoted the other way round: GHSGMD, GHSMRO, GHSNGN, GHSSLL, GHSXOF. Probably foreign units per cedi (to confirm on the Bank of Ghana page)
- WAUGHS (West African Unit of Account) has only 4 decimals and buying = selling, so its daily changes are mostly zero or noise
- No units section in the response. Probably cedis per one foreign unit for the XXXGHS pairs (to confirm, including whether quotes like JPY are per 1 or per 100 units)
- No date-range parameter seen. The API appears to return the whole window each time (to confirm)
- Revision policy: not stated by the source (unknown)

## Data contract (what "valid" means)

- Required fields: rate_date, pair, buying, selling, mid
- Types: date, text, number, number, number
- Rules:
  1. buying > 0
  2. selling >= buying
  3. mid is between buying and selling, and within 0.0001 of their average
  4. no duplicate rate_date + pair
  5. rate_date is a valid date and not in the future
  6. pair is six letters, with exactly one side being GHS
  7. each business day has all 19 pairs

## Layers

- Bronze: the raw JSON file exactly as received (saved with curl.exe), named by the fetch date, e.g. bronze_2026-11-20.json. Never edited
- Silver: one typed row per pair and day, with columns rate_date, pair, base, quote, buying, selling, mid, fetched_at. Keep all 19 pairs
- Gold:
  - every pair put on one basis (cedis per one foreign unit) by inverting the GHSxxx pairs
  - day-over-day change in mid
  - 7-day average mid rate
  - spread (selling - buying)
  - WAUGHS excluded from change metrics or flagged

## Cadence

- The source adds one day per business day and runs 1-2 days late
- Run once a day, fetch the whole response, and update Silver rows by key
- Keep every day's Bronze file. This builds long-term history and shows any later revisions
- A value that changes for an old date is logged as a revision

## What could go wrong

- API down or slow -> retry, then fail the run and keep the last good data
- A field renamed or removed -> reject the batch, keep the Bronze copy
- Fewer than 19 pairs for a day -> flag the day, do not publish it to Gold
- The window moves forward and old rows disappear -> Bronze files keep them
- A past value changes -> record it as a revision, do not overwrite silently
- GHSxxx pairs compared without inverting -> wrong numbers in Gold
- Rate limit (60/minute) -> one request a day is far below it
- Weekends and Ghana public holidays have no data -> not an error
