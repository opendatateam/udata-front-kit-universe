const POOL_SIZE = 5;
const RESOLVABLE = ["dataservice", "dataset", "organization", "topic"];
const MAX_RETRIES = 3;
const RETRY_BASE_DELAY_MS = 1000;
const MIN_REQUEST_INTERVAL_MS = 250; // paces requests regardless of POOL_SIZE; tune down if this still trips the rate limit

let nextRequestTime = 0;


ready(() => {
  grist.ready({requiredAccess: "full"});
});


function ready(fn) {
  if (document.readyState !== "loading") {
    fn();
  } else {
    document.addEventListener("DOMContentLoaded", fn);
  }
}


async function handleClick(btn) {
  btn.innerHTML = 'En cours... <span class="spinner"></span>';
  btn.disabled = true;

  try {
    await synchronize();
  } catch (err) {
    console.error("DatagouvSync:", err);
  }

  btn.innerHTML = "Synchroniser";
  btn.disabled = false;
}


async function synchronize() {
  console.log(`DatagouvSync: Synchronising...`);

  const tableId = await grist.selectedTable.getTableId();
  const env = tableId.toLowerCase().startsWith("prod") ? "www" : "demo";

  const data = await grist.docApi.fetchTable(tableId);
  if (data.id.length == 0) {
    console.log(`DatagouvSync: Nothing in grist`);
    return;
  }

  const rows = cols2rows(data);
  const results = await pooled(POOL_SIZE, rows, row => resolve(env, row));
  const resolved = results.filter(Boolean);
  if (resolved.length == 0) {
    console.log(`DatagouvSync: Nothing to update`);
    return;
  }

  const cols = rows2cols(resolved);
  try {
    await grist.docApi.applyUserActions([
      ["BulkUpdateRecord", tableId, cols.id, { Label: cols.Label, URL: cols.URL }]
    ]);
    console.log(`DatagouvSync: Updated ${cols.id.length} row(s)`);
  } catch (err) {
    console.error("DatagouvSync: Failed to update table:", err);
  }
}


async function resolve(env, row) {
  const type = row.Type.trim().toLowerCase();
  if (!RESOLVABLE.includes(type)) {
    return;
  }

  const identifier = row.Identifiant.trim();
  const object = `${type}s`
  const version = type == "topic" ? "2" : "1";
  const url = `https://${env}.data.gouv.fr/api/${version}/${object}/${identifier}/`;

  let label, resultUrl;
  try {
    const response = await fetchWithRetry(url);
    if (response.ok) {
      const result = await response.json();
      // fields used here must be declared in the X-Fields request header in fetchWithRetry
      label = result.name || result.title || "<missing>";
      resultUrl = result.page || result.self_web_url || result.uri || "<missing>";
    } else {
      console.warn(`DatagouvSync: Failed request for ${object}/${identifier}: ${response.statusText || response.status}`);
      return;
    }
  } catch (err) {
    console.error(`DatagouvSync: Error processing ${object}/${identifier}:`, err);
    return;
  }

  console.log(`DatagouvSync: Result for ${object}/${identifier}: label="${label}", url=${resultUrl}`);
  return {id: row.id, Label: label, URL: resultUrl};
}


async function fetchWithRetry(url) {
  for (let attempt = 0; ; attempt++) {
    await throttle();
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        "X-Fields": "name,page,self_web_url,title,uri"
      }
    });
    if (response.status !== 429 || attempt >= MAX_RETRIES) {
      return response;
    }

    const delayMs = RETRY_BASE_DELAY_MS * 2 ** attempt;
    console.warn(`DatagouvSync: Rate limited on ${url}, retrying in ${delayMs}ms (attempt ${attempt + 1}/${MAX_RETRIES})`);
    await sleep(delayMs);
  }
}


function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}


// Spaces out request starts across the whole pool, independent of POOL_SIZE,
// so we don't dispatch requests faster than the server's rate limit allows.
async function throttle() {
  const now = Date.now();
  const scheduled = Math.max(now, nextRequestTime);
  nextRequestTime = scheduled + MIN_REQUEST_INTERVAL_MS;
  if (scheduled > now) await sleep(scheduled - now);
}


function cols2rows(cols) {
  return Object.values(cols)[0].map((_, i) =>
    Object.fromEntries(
      Object.entries(cols).map(([col, values]) => [col, values[i]])
    )
  );
}


function rows2cols(rows) {
  return Object.fromEntries(
    Object.keys(rows[0]).map(col => [col, rows.map(row => row[col])])
  );
}


async function pooled(limit, array, fn) {
  const results = [];
  const executing = new Set();

  for (const item of array) {
    const p = Promise.resolve().then(() => fn(item));
    results.push(p);
    executing.add(p);
    p.finally(() => executing.delete(p));
    if (executing.size >= limit) await Promise.race(executing);
  }

  return Promise.all(results);
}
