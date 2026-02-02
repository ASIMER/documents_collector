# API Reference — data.rada.gov.ua

> This file contains actual API response structures, verified through Postman on 2026-02-01.
> Use it as the single source of truth when implementing the collector.

---

## 1. Authentication

Single mechanism — HTTP header `User-Agent`.

```
User-Agent: OpenData          → free access (JSON lists, cards, TXT texts)
User-Agent: <24h-token>       → full access (token from https://data.rada.gov.ua/api/token)
```

**We use `OpenData`.** This is sufficient for all our needs.

### What OpenData Provides vs Token

| Capability | OpenData | Token |
|------------|----------|-------|
| Document list (`page{N}.json`) | ✅ | ✅ |
| Dokid list (`docs.json`) | ✅ | ✅ |
| Document card (`card/{nreg}.json`) | ✅ | ✅ |
| JSON with text (`show/{nreg}.json`) | ⚠️ Without `stru` | ✅ With `stru` |
| Plain text (`show/{nreg}.txt`) | ✅ | ✅ |

**CRITICAL:** With `OpenData` token, the `/laws/show/{nreg}.json` endpoint returns **the same response as `/laws/card/{nreg}.json`** — i.e., metadata WITHOUT document body (field `stru` is absent). We get text ONLY through the `.txt` endpoint.

---

## 2. Rate Limits

| Limit | Value |
|-------|----------|
| Requests/minute | up to 60 |
| Recommended pause | random 5–7 sec between requests |
| Requests/day | up to 100,000 |
| Bytes/day | up to 200 MB |
| Pages/day | up to 800,000 |

### Forbidden

- Request token before each request
- Check limits before each request
- Any preflight requests for authorization

Violation → IP blocking.

### Rate Limiter Implementation

```python
import random
import time

def rate_limited_request(session, url, min_pause=5.0, max_pause=7.0):
    """Single request with rate limit compliance."""
    response = session.get(url)
    pause = random.uniform(min_pause, max_pause)
    time.sleep(pause)
    return response
```

---

## 3. Endpoints

### 3.1 List of Updated Documents (with metadata)

```
GET https://data.rada.gov.ua/laws/main/r/page{N}.json
Headers: User-Agent: OpenData
```

**Actual Response (page1.json):**

```json
{
  "cnt": 514,
  "from": 1,
  "max": 514,
  "list": [
    {
      "dokid": 551704,
      "orgid": 70,
      "minjust": "",
      "n_vlas": "",
      "nazva": "On Accounting Price of Banking Metals",
      "npix": 2,
      "nreg": "n0044500-26",
      "num": 1,
      "org": 70,
      "organs": "70:20260130:",
      "orgdat": 20260130,
      "poddat": 20260130,
      "pridat": 20260130,
      "status": 0,
      "typ": 95,
      "types": 95
    },
    {
      "dokid": 551707,
      "orgid": 2,
      "minjust": "",
      "n_vlas": "71-р",
      "nazva": "On Approval of Commission Composition...",
      "npix": 2,
      "nreg": "71-2026-р",
      "num": 6,
      "org": 2,
      "organs": "2:20260128:71-р",
      "orgdat": 20260128,
      "orgnum": "71-р",
      "poddat": 20260128,
      "pridat": 20260128,
      "status": 5,
      "typ": 6,
      "types": "6|168"
    }
  ]
}
```

#### Key Fields from list[]

| Field | Type | Description |
|------|-----|------|
| `dokid` | `int` | Unique document identifier (PK) |
| `nreg` | `string` | System registration number (for URL) |
| `nazva` | `string` | Document title |
| `status` | `int` | Document status (→ stan dictionary) |
| `typ` | `int` | Main document type |
| `types` | `int \| string` | **⚠️ WARNING:** Can be `int` (95) OR `string` with delimiter `\|` ("6\|168", "2\|25\|99") |
| `orgid` | `int` | Publisher organization ID |
| `org` | `int` | Organization ID (duplicates orgid) |
| `orgdat` | `int` | Document date in format `YYYYMMDD` |
| `poddat` | `int` | Current revision date in format `YYYYMMDD` |
| `pridat` | `int` | Adoption date in format `YYYYMMDD` |
| `n_vlas` | `string \| int` | Document number (own) — can be string or number |
| `npix` | `int` | Number of pixel images (not important for us) |

#### Pagination

Currently (2026-02-01) all 514 documents are returned on a single page (`cnt=514, max=514`). But code MUST support pagination:

```python
def collect_all_pages(session, base_url):
    """Collect all pages with pagination."""
    all_docs = []
    page = 1
    while True:
        url = f"{base_url}/laws/main/r/page{page}.json"
        data = rate_limited_request(session, url).json()
        all_docs.extend(data["list"])
        if len(all_docs) >= data["max"]:
            break
        page += 1
    return all_docs
```

#### Parsing types

```python
def parse_types(types_value) -> list[int]:
    """Parse types field which can be int or pipe-separated string."""
    if isinstance(types_value, int):
        return [types_value]
    if isinstance(types_value, str):
        return [int(t) for t in types_value.split("|") if t.strip()]
    return []
```

---

### 3.2 List of Updated Document IDs

```
GET https://data.rada.gov.ua/laws/main/r/docs.json
Headers: User-Agent: OpenData
```

**Actual Response:** Simple JSON array of dokids.

```json
[551704, 551702, 551662, 551661, 551708, 551707, ...]
```

Returns ~514 IDs. Useful for quick check if new documents appeared (smaller payload than page{N}.json).

---

### 3.3 Document Card (metadata)

```
GET https://data.rada.gov.ua/laws/card/{nreg}.json
Headers: User-Agent: OpenData
```

**Actual Response (for nreg=2344-14, law "On Motor Vehicle Transport"):**

```json
{
  "dokid": 81073,
  "orgid": 1,
  "podid": 0,
  "result": "ok",
  "result_code": 200,
  "nreg": "2344-14",
  "nazva": "On Motor Vehicle Transport",
  "status": 5,
  "typ": 1,
  "types": 1,
  "org": 1,
  "orgdat": 20010405,
  "orgnum": "2344-III",
  "poddat": 20250419,
  "pridat": 20010405,
  "datred": 20250419,
  "datcomp": 20240628,
  "size": 223202,
  "pages": 4,
  "format": 2,
  "edcnt": 38,
  "comped": 35,
  "n_vlas": "2344-III",
  "minjust": "",
  "perv": 1,
  "pidstava": "4337-20",
  "tags": 1,
  "temy": 7,
  "termcnt": 167,
  "npix": 18,
  "anotcnt": 1,
  "anots": [
    {
      "ann_id": 81073,
      "ann_dat": 20010405,
      "ann_lang": "uk",
      "ann_nam": "On Motor Vehicle Transport",
      "ann_url": null,
      "nreg": "2344-14"
    }
  ],
  "eds": [...],
  "hist": [...],
  "terms": [75381, 4908, 4911, ...],
  "klasy": "43|801",
  "komitet": 26,
  "publics": "20010511:19:2001 р., № 17...",
  "lang": {"en": "/data/docs/texts/pere/en/d81073_en.htm"},
  "pere": [...],
  "sig": [...]
}
```

#### Fields to Save in Our DB

| Field | Save? | Note |
|------|-------------|----------|
| `dokid` | ✅ PK | Unique identifier |
| `nreg` | ✅ UNIQUE | For building URL |
| `nazva` | ✅ | Document title |
| `status` | ✅ | FK to stan dictionary |
| `typ` | ✅ | Main type |
| `types` | ✅ | All types (parse `\|`) |
| `org` / `orgid` | ✅ | Publisher organization |
| `orgdat` | ✅ | Document date |
| `poddat` | ✅ | Current revision date |
| `pridat` | ✅ | Adoption date |
| `datred` | ✅ | Revision date |
| `size` | ✅ | Size in bytes |
| `edcnt` | ✅ | Number of revisions |
| `anots` | ⚠️ optional | English title (may be useful for LLM) |
| `temy` | ⚠️ optional | Theme ID |
| `hist`, `eds`, `terms` | ❌ | Too detailed |
| `sig` | ❌ | Technical files (CSS, GIF) |

---

### 3.4 Full Document JSON (with OpenData = identical to card)

```
GET https://data.rada.gov.ua/laws/show/{nreg}.json
Headers: User-Agent: OpenData
```

**WARNING:** With `OpenData` token, response is IDENTICAL to `/laws/card/{nreg}.json`. Field `stru` (structured text) is NOT returned. To get text, use `.txt` endpoint.

---

### 3.5 Plain Document Text ✅ PRIMARY TEXT SOURCE

```
GET https://data.rada.gov.ua/laws/show/{nreg}.txt
Headers: User-Agent: OpenData
```

**Actual Response (for nreg=n0044500-26):**

```
NATIONAL BANK OF UKRAINE
NOTIFICATION
30.01.2026
Accounting price of banking metals as of 30.01.2026
Digital code
Letter code
Number of troy ounces
Banking metal name
Accounting price*
959
XAU
1
Gold
237173.05
...
```

Returns **plain text** without HTML tags. Encoding — UTF-8.

**For small documents** (NBU notifications, etc.) text can be short.
**For large laws** (2344-14) — tens of thousands of characters.

#### Handling Empty Text

Some documents may not have text (card only). In this case, `.txt` endpoint may return empty string or error. Always check:

```python
def fetch_document_text(session, base_url, nreg):
    """Get document text."""
    url = f"{base_url}/laws/show/{nreg}.txt"
    response = rate_limited_request(session, url)
    if response.status_code != 200:
        return None
    text = response.text.strip()
    return text if len(text) > 0 else None
```

---

## 4. Dictionary Collection Workflow

### 4.1 What Are Dictionaries?

**Dictionaries** are reference data that documents link to via foreign keys:

- **Statuses** (stan): Document lifecycle states
  - 0 = Not defined
  - 1 = Lost force
  - 2 = Coming into force
  - 3 = Suspended
  - 4 = Resumed
  - 5 = Active
  - 6 = Did not come into force
  - 7 = Not applicable in Ukraine

- **Themes** (temy): Thematic categories
  - Variable count (economy, education, healthcare, etc.)
  - Used for topical classification

- **Types** (typy): Document classifications
  - Law, resolution, decision, notification, etc.
  - Not available via API (404), inferred from documents

- **Organizations** (orgs): Publishing entities
  - Parliament, Cabinet, ministries, etc.
  - Not available via API (404)

### 4.2 Why Two-Step Process (JSON → CSV)?

The Rada API provides dictionaries through a two-step process:

**Step 1: Metadata Endpoint** (JSON)
- `GET /open/data/stan.json`
- Returns: metadata about the dictionary (description, last update, CSV URL)
- Purpose: Discovery and versioning
- Contains `item[0].link` pointing to CSV file

**Step 2: Data Endpoint** (CSV)
- `GET /ogd/zak/laws/data/csv/stan.txt`
- Returns: actual dictionary data as CSV
- Format: Tab-separated values, Windows-1251 encoding
- **Note**: Despite `.txt` extension, content is CSV format

### 4.3 Why CSV with .txt Extension?

The API serves CSV files with `.txt` extension for several reasons:
- Legacy API design decision
- Ensures files open in text editors without special handling
- Prevents Excel auto-formatting issues (dates, leading zeros)
- Maintains compatibility with older systems
- Browser downloads files correctly without MIME type confusion

**Important**: Always parse as CSV (tab-delimited), not as plain text!

### 4.4 Available Dictionaries (Verified 2026-02-01)

| Endpoint | Status | CSV File | Encoding | Format |
|----------|--------|----------|----------|--------|
| `/open/data/stan.json` | ✅ Works | `stan.txt` — document statuses | windows-1251 | Tab-separated |
| `/open/data/temy.json` | ✅ Works | `temy.txt` — document themes | windows-1251 | Tab-separated |
| `/open/data/typs.json` | ❌ 404 Not Found | — | — | — |
| `/open/data/orgs.json` | ❌ 404 Not Found | — | — | — |

### 4.5 Dictionary Metadata Response

**Step 1: GET /open/data/stan.json** (metadata)

```json
{
  "id": "stan",
  "type": "data",
  "title": "Document Statuses",
  "language": "uk",
  "path": "/ogd/zak/laws/data/csv/",
  "name": "stan.txt",
  "format": "csv",
  "item": [
    {
      "type": "file",
      "link": "https://data.rada.gov.ua/ogd/zak/laws/data/csv/stan.txt",
      "format": "csv",
      "size": 156
    }
  ],
  "structure": "dict-simple"
}
```

### 4.6 Dictionary Data Response

**Step 2: GET https://data.rada.gov.ua/ogd/zak/laws/data/csv/stan.txt** (data)

```
0	Not defined
1	Lost force
2	Coming into force
3	Suspended
4	Resumed
5	Active
6	Did not come into force
7	Not applicable in Ukraine
```

#### CSV Format Specifications

- **Delimiter**: **TAB** (`\t`)
- **Encoding**: **windows-1251** (NOT UTF-8!)
- **Structure**: `id\tname` (no headers)
- **Line endings**: `\r\n` (Windows style)

### 4.7 Implementation Pattern

```python
import csv

def load_dictionary(session, base_url, dict_name):
    """Two-step dictionary loading."""
    # Step 1: Get metadata
    meta_url = f"{base_url}/open/data/{dict_name}.json"
    meta = rate_limited_request(session, meta_url).json()

    # Step 2: Download CSV file
    csv_url = meta["item"][0]["link"]
    response = rate_limited_request(session, csv_url)

    # WARNING: encoding is windows-1251!
    response.encoding = "windows-1251"
    text = response.text

    # Parse CSV (tab-delimited, no headers)
    result = {}
    for line in text.strip().split("\n"):
        parts = line.split("\t")
        if len(parts) >= 2:
            dict_id = int(parts[0])
            dict_name_value = parts[1].strip()
            result[dict_id] = dict_name_value

    return result
```

### 4.8 Dictionaries vs Documents — Key Differences

| Aspect | Dictionaries | Documents |
|--------|--------------|-----------|
| **Purpose** | Reference data | Actual content |
| **Volume** | Small (~8-100 entries) | Large (~514+ documents) |
| **Update frequency** | Rare (months/years) | Frequent (daily) |
| **Data format** | CSV (windows-1251) | JSON + TXT (UTF-8) |
| **Loading method** | Two-step (metadata → CSV) | Direct API call |
| **Storage** | PostgreSQL + MinIO snapshot | MinIO raw/processed + PostgreSQL metadata |
| **History tracking** | SCD Type 2 | SCD Type 2 |
| **Dependencies** | None | References dictionaries (FK) |

### 4.9 Why Load Dictionaries First?

**Database Integrity**: Documents have foreign key references:
```sql
-- Dictionary must exist first
INSERT INTO dictionaries VALUES ('rada', 'status', 5, 'Active');

-- Then document can reference it
INSERT INTO documents (source, source_id, status_id, ...)
VALUES ('rada', '551704', 5, ...);  -- status_id=5 must exist!
```

**Loading Order in DAG**:
```
1. collect_dictionaries    → PostgreSQL (load status=0..7, themes)
2. snapshot_dictionaries   → MinIO backup (JSON snapshots for history)
3. collect_document_list   → In-memory (dokid list)
4. collect_document_texts  → MinIO raw/ (can now reference statuses)
5. load_metadata_to_db     → PostgreSQL (FK validation succeeds)
```

---

## 5. Date Formats

API returns dates as **integer in YYYYMMDD format**:

```
20260130 → 2026-01-30
20010405 → 2001-04-05
```

#### Parsing

```python
from datetime import date

def parse_date(date_int: int) -> date | None:
    """Parse date from YYYYMMDD format."""
    if not date_int or date_int == 0:
        return None
    s = str(date_int)
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
```

---

## 6. Text Encoding

| Endpoint | Encoding |
|----------|-----------|
| `.json` endpoints | UTF-8 (standard JSON) |
| `.txt` endpoints | UTF-8 |
| Dictionary CSV (`stan.txt`, `temy.txt`) | **windows-1251** |

**WARNING:** `nazva` in JSON (for large documents) may contain broken encoding (mojibake), e.g., `"ÐŸÑ€Ð¾ Ð°Ð²Ñ‚Ð¾Ð¼Ð¾Ð±Ñ–Ð»ÑŒÐ½Ð¸Ð¹"` instead of `"Про автомобільний"`. This is an API issue (UTF-8 bytes interpreted as windows-1251). Title is better taken from list endpoint (`page{N}.json`) where it is correct.

---

## 7. HTTP Headers for Traffic Optimization

API supports `Last-Modified` / `If-Modified-Since`:

```python
# First request
response = session.get(url)
last_modified = response.headers.get("Last-Modified")

# Next request
headers = {"If-Modified-Since": last_modified}
response = session.get(url, headers=headers)
if response.status_code == 304:
    # Document not changed, skip
    pass
```

---

## 8. Data Collection Strategy

### Complete Cycle for One Document:

```
1. From page{N}.json list → get dokid, nreg, metadata
2. GET /laws/show/{nreg}.txt → get text
3. Save metadata → PostgreSQL
4. Save raw text → MinIO raw/{source}/{dokid}.txt
5. Transform text → Markdown → MinIO processed/{source}/{dokid}.md
```

### Why Not card/{nreg}.json?

The `page{N}.json` endpoint already returns all needed metadata for each document in the list. Separate `card/{nreg}.json` request is only needed if more details are required (editions, history, terms). For LLM training, fields from list are sufficient.

### Volume Estimate

- 514 documents in updated list
- ~2 requests per document (list already obtained + txt)
- With 5-7 sec pause: ~514 × 6 = ~51 minutes for text
- List: 1 request (all on one page)
- Dictionaries: 2-4 requests
- **Total: ~520 requests, ~55 minutes**

---

## 9. Edge Cases and Known Issues

1. **`types` can be int or string** — always parse through `parse_types()`
2. **`n_vlas` can be int or string** — save as string
3. **`orgid` can be string with `|`** — e.g., `"471|164|188|171|261|267|291|330|360|384|121"` for international agreements
4. **`nazva` in card/show JSON** — may be mojibake for large documents, use title from list
5. **Some documents without text** — `.txt` may return empty string
6. **Dictionaries typs and orgs** — 404 via API metadata, try direct CSV URL
7. **Dates as integer** — `0` means no date
8. **CSV dictionaries** — windows-1251, tab-separated, no headers

---

## 10. API Token Authentication (Optional)

**Token vs OpenData Comparison:**

| Feature | OpenData (Free) | Token (24h validity) |
|---------|----------------|----------------------|
| Access to document list | ✅ | ✅ |
| Plain text (.txt) | ✅ | ✅ |
| Basic metadata | ✅ | ✅ |
| `stru` field (structured text in JSON) | ❌ | ✅ |
| Rate limits | Same | Same |
| Authentication | `User-Agent: OpenData` | `User-Agent: <token>` |

### When to Use Token?

**Current implementation**: Using OpenData is sufficient because:
- Plain text available via `.txt` endpoint
- All metadata available in JSON responses
- `stru` field not needed for LLM training (plain text is better)

**Future use case for token**:
- If structured document format needed (HTML with markup)
- If API changes and restricts OpenData access
- If `stru` field provides additional value

### Token Configuration

**Environment variable**:
```bash
RADA_API_TOKEN=f822823f-ff12-4583-8160-0fd9db40507f
```

**Configuration** (`configs/config.yaml` or DAG `config.yml`):
```yaml
sources:
  rada:
    api:
      auth_method: "token"  # or "opendata"
      token: "${RADA_API_TOKEN}"
      fallback_to_opendata: true
```

**Collector logic** (`pipeline/collectors/rada.py`):
```python
def _get_headers(self) -> dict:
    """Get HTTP headers for API requests."""
    if self.use_opendata or not self.token:
        return {"User-Agent": "OpenData"}
    return {"User-Agent": self.token}
```

### Token Validity

- **Duration**: 24 hours from generation
- **Renewal**: Request new token daily from https://data.rada.gov.ua/api/token
- **Fallback**: If token expires, automatically fall back to OpenData
- **Storage**: Store in environment variable, not in code

### Token Request Example

```python
import requests

def get_token():
    """Request new 24-hour token from Rada API."""
    response = requests.get("https://data.rada.gov.ua/api/token")
    token_data = response.json()
    return token_data["token"]
```

**Note**: Token not needed for current implementation. OpenData access is sufficient.

---

## Summary

**Key Takeaways:**
1. Use `OpenData` token for authentication (sufficient for text collection)
2. Dictionaries: two-step process (JSON metadata → CSV download)
3. CSV encoding is **windows-1251**, not UTF-8
4. CSV has `.txt` extension but is tab-delimited format
5. Documents reference dictionaries → load dictionaries first
6. Plain text via `.txt` endpoint (not JSON `stru` field)
7. Dates as integers in `YYYYMMDD` format
8. Rate limit: random 5-7s pause between requests
9. Pagination supported (currently all on one page)
10. `types` field can be int OR pipe-separated string
