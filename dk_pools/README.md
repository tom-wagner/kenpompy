# `dk_pools` Logic Summary

This folder contains a small DraftKings bracket-pool workflow:

1. `example_entry.csv` holds one or more bracket entries in a spreadsheet-friendly format.
2. `csv_to_requests.py` converts each fully resolved CSV row into the JSON payload DraftKings expects.
3. `generated_requests.json` is the output payload list.
4. `place_entries.js` submits those payloads to the DraftKings pool entry API from a logged-in browser session.
5. `dk.json` is reference data captured from DraftKings for the pool and bracket structure.
6. `example_entry.json` is a known-good payload template, mainly used to preserve non-bracket picks such as the tiebreaker.

## Files and Roles

### `csv_to_requests.py`

This is the main transformation step. It reads a CSV row, extracts bracket picks, preserves any extra picks from a template JSON file, and writes a list of request payloads.

The command-line defaults point at the sample files in this folder:

```python
parser.add_argument(
    "input_csv",
    nargs="?",
    default=str(here / "example_entry.csv"),
)
parser.add_argument(
    "-o",
    "--output",
    default=str(here / "generated_requests.json"),
)
parser.add_argument(
    "-t",
    "--template-json",
    default=str(here / "example_entry.json"),
)
```

#### How CSV columns are discovered

Only columns whose names look like `<ROUND>:<pickItemId>` are treated as bracket picks. The code does not hardcode specific round names like `R64` or `R32`; it accepts any all-caps alphanumeric prefix before the colon.

```python
PICK_COLUMN_RE = re.compile(r"^[A-Z0-9]+:(\d+)$")

def pick_columns(fieldnames):
    columns = []
    for fieldname in fieldnames:
        match = PICK_COLUMN_RE.match(fieldname)
        if match:
            columns.append((fieldname, match.group(1)))
    return columns
```

Example from `example_entry.csv`:

```csv
entryGroupKey,pickSetId,R64:210265,R64:210266,...,R2:210327
ed6b9ec9-7dcf-4f5b-b277-cfe76e968e67,13132,16:Siena:429903,9:TCU:429905,...,3:Gonzaga:429962
```

Each bracket column embeds the `pickItemId` in the header. That ID lines up with DraftKings metadata in `dk.json`.

#### How cell values are parsed

Each CSV cell must already be resolved to the format:

```text
<seed>:<team label>:<pickItemOutcomeId>
```

The parser only extracts the final numeric outcome ID:

```python
RESOLVED_PICK_RE = re.compile(r"^(\d+):(.+):(\d+)$")

def parse_resolved_pick(cell_value):
    if cell_value is None:
        return None
    match = RESOLVED_PICK_RE.match(cell_value.strip())
    if not match:
        return None
    return int(match.group(3))
```

So a cell like:

```text
16:Siena:429903
```

becomes:

```json
{
  "pickItemId": "210265",
  "pickItemOutcomeIds": [429903]
}
```

#### How the payload is built

For every recognized pick column, the script creates the DraftKings request shape:

```python
picks.append(
    {
        "pickItemId": pick_item_id,
        "pickItemOutcomeIds": [outcome_id],
    }
)
```

Then it wraps the picks in a payload for the target entry:

```python
payload = {
    "entryGroupKeys": [row["entryGroupKey"]],
    "pickSetId": int(row["pickSetId"]),
    "picks": picks + template_extras,
}
```

Important details:

- `entryGroupKey` comes directly from the CSV row and identifies the DraftKings entry slot being filled.
- `pickSetId` comes from the row and must match the pool’s pick set.
- `template_extras` are appended after the bracket picks.

#### Template extras and the tiebreaker

`load_template_extras()` copies any picks from `example_entry.json` whose `pickItemId` does not appear in the CSV headers:

```python
for pick in template.get("picks", []):
    pick_item_id = str(pick.get("pickItemId"))
    if pick_item_id in csv_pick_item_ids:
        continue
    extras.append(pick)
```

In the sample data, this preserves the final tiebreaker-style answer:

```json
{
  "pickItemId": "210328",
  "pickItemOutcomeIds": null,
  "pickAnswer": 160
}
```

That is why `generated_requests.json` ends up with `64` picks even though the bracket itself has `63` games: `63` matchup picks plus `1` extra answer pick.

#### Missing or unresolved picks

If any bracket cell in a row does not match the resolved format, that row is considered incomplete and is skipped:

```python
if outcome_id is None:
    missing.append(column_name)
    continue

if missing:
    return None, missing
```

By default, incomplete rows are reported to `stderr` and ignored. With `--strict`, the script exits with status `1` instead.

That behavior matters for the sample CSV:

- The first row is fully resolved and converts successfully.
- Rows like `high_seed_r64` and `low_seed_r64` contain placeholders such as `DUKE / OSU` or blank cells, which do not match the required `seed:team:outcomeId` format.

### `place_entries.js`

This file is the submission step. It is meant to be run in a browser context where DraftKings session cookies already exist.

The API target and request configuration are fixed:

```js
const DK_POOL_ENTRY_URL =
  "https://gaming-us.draftkings.com/sites/US-SB/api/pools/v1/ftp/poolentries.json?productContext=sbwebdesktop";

const REQUEST_OPTIONS = {
  method: "PUT",
  credentials: "include",
  mode: "cors",
  referrer: "https://sportsbook.draftkings.com/",
};
```

Each payload is submitted with `fetch()`:

```js
async function placeEntry(payload) {
  const response = await fetch(DK_POOL_ENTRY_URL, {
    ...REQUEST_OPTIONS,
    headers: REQUEST_HEADERS,
    body: JSON.stringify(payload),
  });
```

The response body is read as text first, then parsed as JSON when possible:

```js
const rawText = await response.text();
let data;
try {
  data = rawText ? JSON.parse(rawText) : null;
} catch {
  data = rawText;
}
```

The script then submits entries sequentially with a 500 ms delay:

```js
for (const payload of PICKS) {
  const result = await placeEntry(payload);
  results.push(result);

  if (!result.ok) {
    console.warn("Stopping after failed submission.");
    break;
  }

  await sleep(DELAY_MS);
}
```

Important operational behavior:

- `PICKS` is empty by default and must be manually populated before running.
- The script stops at the first non-OK response.
- It logs both per-entry results and a final summary table.

### `dk.json`

This file is reference metadata from DraftKings rather than executable logic. It explains how the IDs in the CSV and payloads map to the actual bracket.

Useful fields:

- `poolDetails.pickSetId` is `13132`.
- `poolPickItemDetails.maxTotalPicks` is `64`.
- `poolPickItemDetails.matchBracketPickItems` has `63` matchup entries.
- Each matchup includes:
  - `pickItemId`
  - `roundNumber`
  - `childPickItemId`
  - `matchOutcomePickItemOutcomes`

Representative snippet:

```json
{
  "pickItemId": 210265,
  "roundNumber": 1,
  "childPickItemId": 210297,
  "matchOutcomePickItemOutcomes": [
    {
      "pickItemOutcomeId": 429903,
      "teamCode": "SIE",
      "seed": 16
    },
    {
      "pickItemOutcomeId": 429904,
      "teamCode": "DUKE",
      "seed": 1
    }
  ]
}
```

This directly matches the CSV/payload relationship:

- Header `R64:210265` identifies the matchup pick item.
- Cell value `16:Siena:429903` identifies the selected outcome for that matchup.
- The generated request becomes `{"pickItemId":"210265","pickItemOutcomeIds":[429903]}`.

### `example_entry.json`

This is a complete example of a successful submission payload. It serves two purposes:

- It shows the exact request body shape DraftKings accepts.
- It provides the extra non-bracket pick(s) that the CSV does not encode.

### `generated_requests.json`

This is the materialized output of `csv_to_requests.py`. It is a JSON array because the CSV can contain multiple entry rows, each producing one DraftKings request payload.

## End-to-End Flow

The logic across the folder is:

1. Start with DraftKings bracket metadata in `dk.json` to understand `pickItemId` and `pickItemOutcomeId` values.
2. Encode bracket selections in `example_entry.csv` using resolved `seed:team:outcomeId` cells.
3. Run `csv_to_requests.py` to turn valid rows into DraftKings request payloads.
4. Preserve non-CSV extras like the tiebreaker from `example_entry.json`.
5. Submit the generated payloads with `place_entries.js`.

In practice, the conversion step can be run as:

```bash
python3 dk_pools/csv_to_requests.py
```

And the JavaScript submission file expects something like:

```js
const PICKS = [
  ...generatedPayloads
];
```

where `generatedPayloads` is the array from `generated_requests.json`, pasted into the browser script or console.

## Key Constraints

- CSV rows must be fully resolved; placeholders are not accepted.
- The CSV only carries matchup picks, not extra answer fields like the tiebreaker.
- The template JSON is the source of truth for those extra picks.
- Submission depends on an authenticated DraftKings browser session because `place_entries.js` uses `credentials: "include"`.
