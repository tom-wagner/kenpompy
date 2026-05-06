// 03/19/2026 NOTE: This worked as is

const PICKS = [];

const DK_POOL_ENTRY_URL =
  "https://gaming-us-ia.draftkings.com/sites/US-IA-SB/api/poolentry/v1/poolentries.json?productContext=sbwebdesktop";

const REQUEST_HEADERS = {
  accept: "*/*",
  "accept-language": "en-US,en;q=0.9,la;q=0.8",
  "content-type": "application/json",
  priority: "u=1, i",
  "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": '"macOS"',
  "sec-fetch-dest": "empty",
  "sec-fetch-mode": "cors",
  "sec-fetch-site": "same-site",
};

const REQUEST_OPTIONS = {
  method: "PUT",
  credentials: "include",
  mode: "cors",
  referrer: "https://sportsbook.draftkings.com/",
};

const DELAY_MS = 500;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function placeEntry(payload) {
  const response = await fetch(DK_POOL_ENTRY_URL, {
    ...REQUEST_OPTIONS,
    headers: REQUEST_HEADERS,
    body: JSON.stringify(payload),
  });

  const rawText = await response.text();
  let data;
  try {
    data = rawText ? JSON.parse(rawText) : null;
  } catch {
    data = rawText;
  }

  return {
    ok: response.ok,
    status: response.status,
    entryGroupKey: payload.entryGroupKeys?.[0] ?? null,
    data,
  };
}

(async () => {
  if (!Array.isArray(PICKS) || PICKS.length === 0) {
    throw new Error("Set PICKS to a non-empty array of request payloads before running this script.");
  }

  const results = [];

  for (const payload of PICKS) {
    console.log("Submitting entry", payload.entryGroupKeys?.[0] ?? "(unknown)");
    const result = await placeEntry(payload);
    results.push(result);
    console.log(result);

    if (!result.ok) {
      console.warn("Stopping after failed submission.");
      break;
    }

    await sleep(DELAY_MS);
  }

  console.log("Finished submitting entries.");
  console.table(
    results.map((result) => ({
      entryGroupKey: result.entryGroupKey,
      status: result.status,
      ok: result.ok,
    }))
  );
})();
