const CONFIG = {
  symbol: process.env.SIGNAL_SYMBOL || "XAUUSD",
  threshold: Number(process.env.SIGNAL_THRESHOLD || 80),
  dataProvider: (process.env.SIGNAL_DATA_PROVIDER || "bridge").toLowerCase(),
  bridgeUrl: process.env.SIGNAL_BRIDGE_URL || "http://127.0.0.1:8000",
  allowYahooFallback: process.env.SIGNAL_ALLOW_YAHOO_FALLBACK !== "0",
  allowOffHours: process.env.SIGNAL_ALLOW_OFF_HOURS === "1",
  telegramToken: process.env.TELEGRAM_BOT_TOKEN || "",
  telegramChatId: process.env.TELEGRAM_CHAT_ID || "",
  dryRun: process.env.DRY_RUN === "1" || process.argv.includes("--dry-run"),
  failOnNoSignal: process.env.FAIL_ON_NO_SIGNAL === "1",
  cooldownMinutes: Number(process.env.SIGNAL_COOLDOWN_MINUTES || 45),
  statePath: process.env.SIGNAL_STATE_PATH || ".signal-alert-state.json",
  supabaseUrl: String(process.env.SUPABASE_URL || "").replace(/\/$/, ""),
  supabaseServiceKey: process.env.SUPABASE_SERVICE_ROLE_KEY || "",
  cloudMemoryEnabled: process.env.SIGNAL_CLOUD_MEMORY !== "0",
  requireCloudMemory: process.env.SIGNAL_REQUIRE_CLOUD_MEMORY === "1"
};

const SIGNAL_ENGINE_VERSION = "v4";
const SIGNAL_ENTRY_TOUCH_ATR = 0.22;
const SIGNAL_MIN_RR = 1.05;
const SIGNAL_MARKET_TIME_BUFFER_MS = 2 * 60 * 1000;
const SIGNAL_MARKET_OFFSET_MAX_AGE_MS = 12 * 60 * 60 * 1000;
const SIGNAL_OUTCOME_HORIZON_MS = 6 * 60 * 60 * 1000;
const SIGNAL_CLOUD_TABLE = "ai_signal_records";
const runtimeDataProviders = new Map();

const TIMEFRAMES = [
  { key: "M1", interval: "1m", range: "1d", weight: 0.65 },
  { key: "M5", interval: "5m", range: "5d", weight: 1.25 },
  { key: "M15", interval: "15m", range: "5d", weight: 2.4 },
  { key: "H1", interval: "1h", range: "1mo", weight: 3.2 }
];

const YAHOO_SYMBOLS = {
  XAUUSD: "GC=F",
  GOLD: "GC=F",
  GC: "GC=F"
};

function clamp(value, min, max){
  return Math.max(min, Math.min(max, value));
}

function formatPrice(value){
  if(!Number.isFinite(value)) return "--";
  return value.toLocaleString("de-DE", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });
}

function formatPercent(value){
  if(!Number.isFinite(value)) return "--";
  return `${Math.round(value)}%`;
}

function formatDirection(direction){
  if(direction === "buy") return "BUY";
  if(direction === "sell") return "SELL";
  return "NEUTRAL";
}

function isTelegramConfigured(){
  return Boolean(CONFIG.telegramToken && CONFIG.telegramChatId);
}

function getProviderSymbol(symbol){
  const normalized = String(symbol || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
  return YAHOO_SYMBOLS[normalized] || process.env.SIGNAL_PROVIDER_SYMBOL || "GC=F";
}

function formatDateForBridge(date){
  const pad = (value) => String(value).padStart(2, "0");
  return [
    date.getFullYear(),
    pad(date.getMonth() + 1),
    pad(date.getDate())
  ].join("-") + "T" + [
    pad(date.getHours()),
    pad(date.getMinutes()),
    pad(date.getSeconds())
  ].join(":");
}

function parseBridgeLocalDate(value){
  const text = String(value || "").trim();
  const localIsoMatch = text.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?$/);

  if(localIsoMatch){
    return new Date(
      Number(localIsoMatch[1]),
      Number(localIsoMatch[2]) - 1,
      Number(localIsoMatch[3]),
      Number(localIsoMatch[4]),
      Number(localIsoMatch[5]),
      Number(localIsoMatch[6] || 0)
    );
  }

  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function getCachedMarketTimeOffset(previousState){
  const offsetMs = Number(previousState && previousState.lastMarketTimeOffsetMs);
  const cachedAt = previousState && previousState.lastMarketTimeOffsetAt
    ? new Date(previousState.lastMarketTimeOffsetAt).getTime()
    : NaN;

  if(
    Number.isFinite(offsetMs) &&
    Number.isFinite(cachedAt) &&
    Date.now() - cachedAt <= SIGNAL_MARKET_OFFSET_MAX_AGE_MS
  ){
    return offsetMs;
  }

  return 0;
}

async function resolveBridgeMarketTimeOffset(previousState){
  if(CONFIG.dataProvider !== "bridge"){
    return 0;
  }

  try{
    const params = new URLSearchParams({symbol: CONFIG.symbol});
    const payload = await fetchJson(`${CONFIG.bridgeUrl.replace(/\/$/, "")}/market/quote?${params.toString()}`, 5000);
    const tickTime = payload && payload.tick ? payload.tick.time : "";
    const brokerDate = parseBridgeLocalDate(tickTime);

    if(!brokerDate){
      return getCachedMarketTimeOffset(previousState);
    }

    const offset = brokerDate.getTime() - Date.now();
    return Math.abs(offset) > 30 * 1000 ? offset : 0;
  }catch(error){
    return getCachedMarketTimeOffset(previousState);
  }
}

function getBridgeRange(timeframe, marketTimeOffsetMs = 0){
  const daysByTimeframe = {
    M1: 0.35,
    M5: 1.8,
    M15: 4,
    H1: 14
  };
  const end = new Date(Date.now() + marketTimeOffsetMs + SIGNAL_MARKET_TIME_BUFFER_MS);
  const start = new Date(end.getTime() - (daysByTimeframe[timeframe.key] || 2) * 24 * 60 * 60 * 1000);

  return {
    from: formatDateForBridge(start),
    to: formatDateForBridge(end)
  };
}

async function fetchJson(url, timeoutMs = 12000){
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try{
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        "User-Agent": "TradingJournalSignalWatcher/1.0"
      }
    });

    if(!response.ok){
      throw new Error(`HTTP ${response.status} fuer ${url}`);
    }

    return await response.json();
  }finally{
    clearTimeout(timer);
  }
}

function normalizeYahooCandles(payload){
  const result = payload && payload.chart && payload.chart.result && payload.chart.result[0];
  if(!result || !Array.isArray(result.timestamp)){
    throw new Error("Keine Yahoo-Chart-Daten erhalten.");
  }

  const quote = result.indicators && result.indicators.quote && result.indicators.quote[0];
  if(!quote){
    throw new Error("Keine Yahoo-OHLC-Daten erhalten.");
  }

  return result.timestamp.map((timestamp, index) => ({
    time: timestamp * 1000,
    open: Number(quote.open[index]),
    high: Number(quote.high[index]),
    low: Number(quote.low[index]),
    close: Number(quote.close[index]),
    volume: Number(quote.volume[index] || 0)
  })).filter((candle) => (
    Number.isFinite(candle.open) &&
    Number.isFinite(candle.high) &&
    Number.isFinite(candle.low) &&
    Number.isFinite(candle.close)
  ));
}

function normalizeBridgeCandles(payload){
  const items = Array.isArray(payload && payload.items) ? payload.items : [];
  return items.map((item) => ({
    time: new Date(item.time).getTime(),
    open: Number(item.open),
    high: Number(item.high),
    low: Number(item.low),
    close: Number(item.close),
    volume: Number(item.tick_volume || item.real_volume || 0)
  })).filter((candle) => (
    Number.isFinite(candle.time) &&
    Number.isFinite(candle.open) &&
    Number.isFinite(candle.high) &&
    Number.isFinite(candle.low) &&
    Number.isFinite(candle.close)
  ));
}

async function loadYahooCandles(timeframe){
  const providerSymbol = encodeURIComponent(getProviderSymbol(CONFIG.symbol));
  const params = new URLSearchParams({
    interval: timeframe.interval,
    range: timeframe.range,
    includePrePost: "false"
  });
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${providerSymbol}?${params.toString()}`;
  const payload = await fetchJson(url);
  const candles = normalizeYahooCandles(payload);

  if(candles.length < 80){
    throw new Error(`${timeframe.key}: zu wenige Kerzen erhalten (${candles.length}).`);
  }

  return candles;
}

async function loadBridgeCandles(timeframe, marketTimeOffsetMs = 0){
  const range = getBridgeRange(timeframe, marketTimeOffsetMs);
  const params = new URLSearchParams({
    symbol: CONFIG.symbol,
    timeframe: timeframe.key,
    from: range.from,
    to: range.to,
    limit: "900"
  });
  const payload = await fetchJson(`${CONFIG.bridgeUrl.replace(/\/$/, "")}/market/candles?${params.toString()}`, 9000);
  const candles = normalizeBridgeCandles(payload);

  if(candles.length < 80){
    throw new Error(`${timeframe.key}: zu wenige Bridge-Kerzen erhalten (${candles.length}).`);
  }

  return candles;
}

async function loadCandles(timeframe, marketTimeOffsetMs = 0){
  if(CONFIG.dataProvider === "bridge"){
    try{
      const candles = await loadBridgeCandles(timeframe, marketTimeOffsetMs);
      runtimeDataProviders.set(timeframe.key, "bridge");
      return candles;
    }catch(error){
      if(!CONFIG.allowYahooFallback){
        throw error;
      }
      const candles = await loadYahooCandles(timeframe);
      runtimeDataProviders.set(timeframe.key, "yahoo");
      return candles;
    }
  }

  if(CONFIG.dataProvider === "yahoo"){
    const candles = await loadYahooCandles(timeframe);
    runtimeDataProviders.set(timeframe.key, "yahoo");
    return candles;
  }

  throw new Error(`Unbekannter SIGNAL_DATA_PROVIDER: ${CONFIG.dataProvider}`);
}

function calculateEmaSeries(candles, period){
  const multiplier = 2 / (period + 1);
  let ema = null;

  return candles.map((candle, index) => {
    if(index < period - 1){
      return { time: candle.time, value: null };
    }

    if(ema === null){
      const slice = candles.slice(index - period + 1, index + 1);
      ema = slice.reduce((sum, item) => sum + item.close, 0) / period;
    }else{
      ema = candle.close * multiplier + ema * (1 - multiplier);
    }

    return { time: candle.time, value: ema };
  });
}

function calculateAtr(candles, period = 14){
  if(candles.length < period + 1){
    return candles.reduce((sum, candle) => sum + (candle.high - candle.low), 0) / Math.max(candles.length, 1);
  }

  const ranges = [];
  for(let index = 1; index < candles.length; index += 1){
    const candle = candles[index];
    const previous = candles[index - 1];
    ranges.push(Math.max(
      candle.high - candle.low,
      Math.abs(candle.high - previous.close),
      Math.abs(candle.low - previous.close)
    ));
  }

  return ranges.slice(-period).reduce((sum, value) => sum + value, 0) / period;
}

function findSwingLevels(candles){
  const sample = candles.slice(-150);
  const lastClose = candles[candles.length - 1].close;
  const swingHighs = [];
  const swingLows = [];

  for(let index = 2; index < sample.length - 2; index += 1){
    const candle = sample[index];
    const leftOne = sample[index - 1];
    const leftTwo = sample[index - 2];
    const rightOne = sample[index + 1];
    const rightTwo = sample[index + 2];

    if(candle.high > leftOne.high && candle.high > leftTwo.high && candle.high > rightOne.high && candle.high > rightTwo.high){
      swingHighs.push(candle.high);
    }

    if(candle.low < leftOne.low && candle.low < leftTwo.low && candle.low < rightOne.low && candle.low < rightTwo.low){
      swingLows.push(candle.low);
    }
  }

  return {
    support: swingLows.filter((level) => level < lastClose).sort((a, b) => b - a)[0] || Math.min(...sample.map((candle) => candle.low)),
    resistance: swingHighs.filter((level) => level > lastClose).sort((a, b) => a - b)[0] || Math.max(...sample.map((candle) => candle.high)),
    rangeHigh: Math.max(...sample.slice(-80).map((candle) => candle.high)),
    rangeLow: Math.min(...sample.slice(-80).map((candle) => candle.low))
  };
}

function analyzeTimeframe(timeframe, candles){
  const last = candles[candles.length - 1];
  const closes = candles.map((candle) => candle.close);
  const ema20Data = calculateEmaSeries(candles, 20);
  const ema50Data = calculateEmaSeries(candles, 50);
  const ema20 = ema20Data[ema20Data.length - 1].value;
  const ema50 = ema50Data[ema50Data.length - 1].value;
  const atr = calculateAtr(candles, 14);
  const levels = findSwingLevels(candles);
  const previousClose = closes[closes.length - 18] || closes[0] || last.close;
  const momentum = atr > 0 ? (last.close - previousClose) / atr : 0;
  const recentHigh = Math.max(...candles.slice(-24).map((candle) => candle.high));
  const recentLow = Math.min(...candles.slice(-24).map((candle) => candle.low));
  const priorHigh = Math.max(...candles.slice(-70, -24).map((candle) => candle.high));
  const priorLow = Math.min(...candles.slice(-70, -24).map((candle) => candle.low));

  let score = 0;
  if(Number.isFinite(ema20) && Number.isFinite(ema50)){
    if(ema20 > ema50) score += 2;
    if(ema20 < ema50) score -= 2;
  }

  if(Number.isFinite(ema20)){
    if(last.close > ema20) score += 1;
    if(last.close < ema20) score -= 1;
  }

  if(recentHigh > priorHigh) score += 1;
  if(recentLow < priorLow) score -= 1;
  if(momentum > 0.45) score += 1;
  if(momentum < -0.45) score -= 1;

  const biasKey = score >= 3 ? "long" : score <= -3 ? "short" : "neutral";
  const confidence = clamp(54 + Math.abs(score) * 8, 42, 92);

  return {
    timeframe: timeframe.key,
    biasKey,
    score,
    confidence,
    signedScore: biasKey === "long" ? Math.abs(score) : biasKey === "short" ? -Math.abs(score) : 0,
    lastPrice: last.close,
    lastCandle: last,
    atr,
    ema20,
    ema50,
    momentum,
    levels
  };
}

function pickNearestLevel(analyses, key, price, fallback){
  const levels = analyses
    .map((analysis) => analysis.levels && analysis.levels[key])
    .filter((value) => Number.isFinite(value));

  if(!levels.length) return fallback;

  if(key === "resistance" || key === "rangeHigh"){
    return levels.filter((value) => value > price).sort((a, b) => a - b)[0] || Math.max(...levels, fallback);
  }

  return levels.filter((value) => value < price).sort((a, b) => b - a)[0] || Math.min(...levels, fallback);
}

function getSessionContext(date = new Date()){
  const hour = Number(new Intl.DateTimeFormat("en-US", {
    timeZone: "Europe/Berlin",
    hour: "2-digit",
    hour12: false
  }).format(date));

  if(hour >= 7 && hour < 13) return "London Session";
  if(hour >= 13 && hour < 17) return "London/New York Overlap";
  if(hour >= 17 && hour < 22) return "New York Session";
  return "Asia / Off Hours";
}

function buildTradePlan(direction, analyses){
  const base = analyses.find((analysis) => analysis.timeframe === "M5") || analyses[0];
  const h1 = analyses.find((analysis) => analysis.timeframe === "H1");
  const m15 = analyses.find((analysis) => analysis.timeframe === "M15");
  const price = base.lastPrice;
  const blendedAtr = Math.max(
    base.atr || 0,
    m15 ? (m15.atr || 0) / 2.6 : 0,
    h1 ? (h1.atr || 0) / 7.5 : 0,
    price * 0.0007
  );
  const support = pickNearestLevel(analyses, "support", price, base.levels.support);
  const resistance = pickNearestLevel(analyses, "resistance", price, base.levels.resistance);
  const rangeHeight = Math.max(Math.abs(resistance - support), blendedAtr * 2.2);
  const projectionMove = Math.max(blendedAtr * 3.4, rangeHeight * 0.34, price * 0.0016);
  const emaAnchor = Number.isFinite(base.ema20) ? base.ema20 : price;

  if(direction === "buy"){
    const entryLow = Math.min(price - blendedAtr * 0.35, emaAnchor - blendedAtr * 0.12);
    const entryHigh = Math.min(price - blendedAtr * 0.08, emaAnchor + blendedAtr * 0.30);
    return {
      entryLow,
      entryHigh,
      invalidation: Math.min(support - blendedAtr * 0.55, entryLow - blendedAtr * 1.15),
      tp1: Math.max(resistance, price + projectionMove),
      tp2: Math.max(resistance, price + projectionMove * 1.72),
      support,
      resistance
    };
  }

  if(direction === "sell"){
    const entryLow = Math.max(price + blendedAtr * 0.08, emaAnchor - blendedAtr * 0.30);
    const entryHigh = Math.max(price + blendedAtr * 0.35, emaAnchor + blendedAtr * 0.12);
    return {
      entryLow,
      entryHigh,
      invalidation: Math.max(resistance + blendedAtr * 0.55, entryHigh + blendedAtr * 1.15),
      tp1: Math.min(support, price - projectionMove),
      tp2: Math.min(support, price - projectionMove * 1.72),
      support,
      resistance
    };
  }

  return {
    entryLow: support,
    entryHigh: resistance,
    invalidation: support,
    tp1: resistance,
    tp2: resistance,
    support,
    resistance
  };
}

function doesCandleTouchRange(candle, low, high, buffer = 0){
  if(!candle) return false;
  const rangeLow = Math.min(Number(low), Number(high)) - buffer;
  const rangeHigh = Math.max(Number(low), Number(high)) + buffer;
  if(!Number.isFinite(rangeLow) || !Number.isFinite(rangeHigh)) return false;
  return Number(candle.high) >= rangeLow && Number(candle.low) <= rangeHigh;
}

function getTradeRewardRisk(trade){
  const entry = (Number(trade.entryLow) + Number(trade.entryHigh)) / 2;
  const risk = Math.abs(entry - Number(trade.invalidation));
  const reward = Math.abs(Number(trade.tp1) - entry);
  return risk > 0 && Number.isFinite(reward) ? reward / risk : 0;
}

function buildSignalGate(signal, context){
  const blockers = [];
  const passes = [];
  const trade = signal.trade;
  const base = context.base;
  const rr = getTradeRewardRisk(trade);
  const entryTouched = doesCandleTouchRange(
    base && base.lastCandle,
    trade.entryLow,
    trade.entryHigh,
    Math.max(Number(base && base.atr) || 0, 1) * SIGNAL_ENTRY_TOUCH_ATR
  );

  if(signal.direction === "neutral"){
    blockers.push("Kein klarer Master-Bias.");
  }

  if(signal.setupQuality !== "A"){
    blockers.push(`Setup-Qualitaet ${signal.setupQuality}, nicht A.`);
  }else{
    passes.push("A-Setup.");
  }

  if(!context.htfAligned){
    blockers.push("H1 und M15 sind nicht sauber aligned.");
  }else{
    passes.push("H1/M15 aligned.");
  }

  if(!context.triggerAligned){
    blockers.push("M5 und M1 bestaetigen den Entry nicht beide.");
  }else{
    passes.push("M5/M1 Trigger aligned.");
  }

  if(!entryTouched){
    blockers.push("Entry-Zone wurde noch nicht erreicht.");
  }else{
    passes.push("Entry-Zone erreicht.");
  }

  if(rr < SIGNAL_MIN_RR){
    blockers.push(`R:R nur 1:${rr.toFixed(2).replace(".", ",")}.`);
  }else{
    passes.push(`R:R 1:${rr.toFixed(2).replace(".", ",")}.`);
  }

  if(!CONFIG.allowOffHours && signal.session === "Asia / Off Hours"){
    blockers.push("Off-Hours-Filter aktiv.");
  }

  return {
    version: SIGNAL_ENGINE_VERSION,
    canSend: blockers.length === 0,
    rr,
    entryTouched,
    blockers,
    passes,
    summary: blockers.length ? blockers[0] : passes.join(" ")
  };
}

function buildMasterSignal(analyses){
  const byTf = Object.fromEntries(analyses.map((analysis) => [analysis.timeframe, analysis]));
  const h1 = byTf.H1;
  const m15 = byTf.M15;
  const m5 = byTf.M5;
  const m1 = byTf.M1;
  const sign = (analysis) => analysis && analysis.biasKey === "long" ? 1 : analysis && analysis.biasKey === "short" ? -1 : 0;
  const h1Sign = sign(h1);
  const m15Sign = sign(m15);
  const m5Sign = sign(m5);
  const m1Sign = sign(m1);
  const htfConflict = h1Sign !== 0 && m15Sign !== 0 && h1Sign !== m15Sign;
  const htfAligned = h1Sign !== 0 && h1Sign === m15Sign;
  const weightedScore = TIMEFRAMES.reduce((total, timeframe) => {
    const analysis = byTf[timeframe.key];
    return total + (analysis ? analysis.signedScore * timeframe.weight : 0);
  }, 0);

  let direction = "neutral";
  if(htfConflict){
    direction = "neutral";
  }else if(htfAligned){
    direction = h1Sign > 0 ? "buy" : "sell";
  }else if(h1Sign !== 0){
    direction = h1Sign > 0 ? "buy" : "sell";
  }else if(m15Sign !== 0){
    direction = m15Sign > 0 ? "buy" : "sell";
  }else if(weightedScore > 1.65){
    direction = "buy";
  }else if(weightedScore < -1.65){
    direction = "sell";
  }

  const masterSign = direction === "buy" ? 1 : direction === "sell" ? -1 : 0;
  const triggerAligned = masterSign > 0 ? m5Sign === 1 && m1Sign === 1 : masterSign < 0 ? m5Sign === -1 && m1Sign === -1 : false;
  const triggerAgainst = masterSign !== 0 && [m5Sign, m1Sign].some((value) => value === -masterSign);
  const confidenceBase = 50 + Math.min(28, Math.abs(weightedScore) * 6.2);
  const confidence = clamp(
    confidenceBase +
    (htfAligned ? 12 : 0) +
    (triggerAligned ? 7 : 0) -
    (htfConflict ? 18 : 0) -
    (triggerAgainst ? 10 : 0),
    38,
    92
  );
  const base = m5 || analyses[0];
  const trade = buildTradePlan(direction, analyses);
  const setupQuality = htfConflict ? "D" : direction === "neutral" ? "C" : htfAligned && triggerAligned ? "A" : triggerAligned ? "B" : "B-";
  const signal = {
    symbol: CONFIG.symbol,
    providerSymbol: CONFIG.dataProvider === "bridge" ? `${CONFIG.symbol}@MT5` : getProviderSymbol(CONFIG.symbol),
    direction,
    confidence,
    weightedScore,
    setupQuality,
    session: getSessionContext(),
    lastPrice: base.lastPrice,
    trade,
    timeframes: analyses,
    createdAt: new Date().toISOString(),
    reason: [
      `H1 ${h1 ? h1.biasKey : "--"}`,
      `M15 ${m15 ? m15.biasKey : "--"}`,
      `M5 ${m5 ? m5.biasKey : "--"}`,
      `M1 ${m1 ? m1.biasKey : "--"}`,
      htfConflict ? "HTF-Konflikt" : htfAligned ? "HTF aligned" : "HTF nicht perfekt",
      triggerAligned ? "Trigger aligned" : triggerAgainst ? "Trigger gegen Bias" : "Trigger fehlt"
    ].join(" | ")
  };

  signal.signalGate = buildSignalGate(signal, {
    base,
    htfAligned,
    triggerAligned,
    triggerAgainst
  });

  return signal;
}

async function buildSignal(previousState = null){
  const marketTimeOffsetMs = await resolveBridgeMarketTimeOffset(previousState);
  runtimeDataProviders.clear();
  const loaded = await Promise.all(TIMEFRAMES.map(async (timeframe) => {
    const candles = await loadCandles(timeframe, marketTimeOffsetMs);
    return {
      timeframe,
      candles,
      analysis: analyzeTimeframe(timeframe, candles)
    };
  }));
  const entries = loaded.map((item) => item.analysis);

  const signal = buildMasterSignal(entries);
  const providers = Array.from(new Set(loaded.map((item) => runtimeDataProviders.get(item.timeframe.key) || CONFIG.dataProvider)));
  const provider = providers.length === 1 ? providers[0] : providers.join("+");
  signal.marketTimeOffsetMs = marketTimeOffsetMs;
  signal.dataProvider = provider;
  signal.providerSymbol = provider === "bridge" ? `${CONFIG.symbol}@MT5` : getProviderSymbol(CONFIG.symbol);
  signal.engineProfile = provider === "bridge" ? "v4-core-mt5" : "v4-core-yahoo";
  signal.candlesByTimeframe = Object.fromEntries(loaded.map((item) => [item.timeframe.key, item.candles]));
  return signal;
}

async function readPreviousState(){
  const fs = await import("node:fs/promises");
  try{
    const text = await fs.readFile(CONFIG.statePath, "utf8");
    return JSON.parse(text);
  }catch(error){
    return null;
  }
}

async function writeState(state){
  const fs = await import("node:fs/promises");
  await fs.writeFile(CONFIG.statePath, `${JSON.stringify(state, null, 2)}\n`, "utf8");
}

function isCloudMemoryConfigured(){
  return Boolean(
    CONFIG.cloudMemoryEnabled &&
    CONFIG.supabaseUrl &&
    CONFIG.supabaseServiceKey
  );
}

async function requestSupabase(path, options = {}){
  if(!isCloudMemoryConfigured()){
    return null;
  }

  const authHeaders = CONFIG.supabaseServiceKey.startsWith("eyJ")
    ? {Authorization: `Bearer ${CONFIG.supabaseServiceKey}`}
    : {};
  const response = await fetch(`${CONFIG.supabaseUrl}/rest/v1/${path}`, {
    method: options.method || "GET",
    headers: {
      apikey: CONFIG.supabaseServiceKey,
      ...authHeaders,
      "Content-Type": "application/json",
      Prefer: options.prefer || "return=representation",
      ...(options.headers || {})
    },
    body: options.body === undefined ? undefined : JSON.stringify(options.body)
  });

  if(!response.ok){
    const body = await response.text();
    throw new Error(`Supabase ${options.method || "GET"} fehlgeschlagen: HTTP ${response.status} ${body}`);
  }

  if(response.status === 204){
    return null;
  }

  const text = await response.text();
  return text ? JSON.parse(text) : null;
}

function buildCloudSignalId(signal){
  const base = signal.timeframes.find((item) => item.timeframe === "M5") || signal.timeframes[0];
  const chartTime = Number(base && base.lastCandle && base.lastCandle.time) || Date.now();
  return [
    SIGNAL_ENGINE_VERSION,
    signal.engineProfile,
    signal.symbol,
    "M5",
    chartTime,
    signal.direction
  ].join(":");
}

function buildCloudSignalRecord(signal){
  const base = signal.timeframes.find((item) => item.timeframe === "M5") || signal.timeframes[0];
  const trade = signal.trade || {};
  const chartTime = Number(base && base.lastCandle && base.lastCandle.time) || Date.now();
  const biasKey = signal.direction === "buy" ? "long" : signal.direction === "sell" ? "short" : "neutral";

  return {
    id: buildCloudSignalId(signal),
    signal_version: SIGNAL_ENGINE_VERSION,
    engine_profile: signal.engineProfile,
    source: "github",
    data_provider: signal.dataProvider,
    provider_symbol: signal.providerSymbol,
    symbol: signal.symbol,
    timeframe: "M5",
    bias_key: biasKey,
    confidence: Math.round(signal.confidence),
    confluence_score: 0,
    setup_quality: signal.setupQuality,
    entry_low: trade.entryLow,
    entry_high: trade.entryHigh,
    entry_mid: (Number(trade.entryLow) + Number(trade.entryHigh)) / 2,
    invalidation: trade.invalidation,
    tp1: trade.tp1,
    tp2: trade.tp2,
    rr: signal.signalGate ? signal.signalGate.rr : getTradeRewardRisk(trade),
    session: signal.session,
    signal_label: "V4 Cloud Signal",
    signal_reason: signal.reason,
    chart_time: chartTime,
    created_at: signal.createdAt,
    entry_touched_at: signal.createdAt,
    entry_confirmed: true,
    outcome: "pending",
    metadata: {
      weightedScore: signal.weightedScore,
      gate: signal.signalGate,
      timeframeBiases: Object.fromEntries(signal.timeframes.map((item) => [item.timeframe, item.biasKey]))
    },
    updated_at: new Date().toISOString()
  };
}

async function saveCloudSignal(signal){
  if(!isCloudMemoryConfigured()){
    return {enabled: false, saved: false};
  }

  const record = buildCloudSignalRecord(signal);
  const path = `${SIGNAL_CLOUD_TABLE}?on_conflict=id`;
  await requestSupabase(path, {
    method: "POST",
    prefer: "resolution=merge-duplicates,return=minimal",
    body: [record]
  });

  return {enabled: true, saved: true, id: record.id};
}

async function loadPendingCloudSignals(){
  if(!isCloudMemoryConfigured()){
    return [];
  }

  const params = new URLSearchParams({
    select: "id,symbol,bias_key,chart_time,created_at,entry_low,entry_high,entry_mid,invalidation,tp1,tp2,rr,entry_confirmed,outcome",
    symbol: `eq.${CONFIG.symbol}`,
    signal_version: `eq.${SIGNAL_ENGINE_VERSION}`,
    outcome: "eq.pending",
    order: "created_at.asc",
    limit: "100"
  });
  const records = await requestSupabase(`${SIGNAL_CLOUD_TABLE}?${params.toString()}`);
  return Array.isArray(records) ? records : [];
}

async function updateCloudSignalOutcome(record, update){
  const params = new URLSearchParams({id: `eq.${record.id}`});
  await requestSupabase(`${SIGNAL_CLOUD_TABLE}?${params.toString()}`, {
    method: "PATCH",
    prefer: "return=minimal",
    body: {
      ...update,
      updated_at: new Date().toISOString()
    }
  });
}

async function resolveCloudSignalOutcomes(signal){
  if(!isCloudMemoryConfigured()){
    return {enabled: false, checked: 0, resolved: 0};
  }

  const pending = await loadPendingCloudSignals();
  const candles = signal.candlesByTimeframe.M1 || signal.candlesByTimeframe.M5 || [];
  let resolved = 0;

  for(const record of pending){
    const chartTime = Number(record.chart_time) || new Date(record.created_at).getTime();
    const futureCandles = candles.filter((candle) => Number(candle.time) > chartTime);
    if(!futureCandles.length){
      continue;
    }

    let outcome = "";
    let resolvedPrice = null;
    let resolvedAt = null;
    for(const candle of futureCandles){
      const hitInvalid = record.bias_key === "long"
        ? candle.low <= Number(record.invalidation)
        : candle.high >= Number(record.invalidation);
      const hitTarget = record.bias_key === "long"
        ? candle.high >= Number(record.tp1)
        : candle.low <= Number(record.tp1);

      if(hitInvalid || hitTarget){
        outcome = hitInvalid && hitTarget ? "ambiguous" : hitInvalid ? "invalid" : "target";
        resolvedPrice = hitInvalid && hitTarget
          ? Number(record.entry_mid)
          : hitInvalid
            ? Number(record.invalidation)
            : Number(record.tp1);
        resolvedAt = new Date(candle.time).toISOString();
        break;
      }
    }

    const latestCandle = futureCandles[futureCandles.length - 1];
    if(!outcome && Number(latestCandle.time) - chartTime > SIGNAL_OUTCOME_HORIZON_MS){
      outcome = "expired";
      resolvedAt = new Date(latestCandle.time).toISOString();
    }

    if(!outcome){
      continue;
    }

    const resultR = outcome === "target"
      ? Number(record.rr) || 1
      : outcome === "invalid"
        ? -1
        : null;
    await updateCloudSignalOutcome(record, {
      outcome,
      resolved_at: resolvedAt,
      resolved_price: resolvedPrice,
      result_r: resultR
    });
    resolved += 1;
  }

  return {enabled: true, checked: pending.length, resolved};
}

function shouldSendAlert(signal, previousState){
  if(signal.direction === "neutral") return { send: false, reason: "Signal ist neutral." };
  if(!signal.signalGate || signal.signalGate.canSend !== true){
    return { send: false, reason: `V4 Gate blockiert: ${signal.signalGate ? signal.signalGate.summary : "Filter nicht bestanden."}` };
  }
  if(signal.confidence < CONFIG.threshold) return { send: false, reason: `Confidence ${formatPercent(signal.confidence)} liegt unter ${CONFIG.threshold}%.` };

  if(previousState && previousState.lastAlertAt && previousState.lastDirection === signal.direction){
    const ageMinutes = (Date.now() - new Date(previousState.lastAlertAt).getTime()) / (1000 * 60);
    if(Number.isFinite(ageMinutes) && ageMinutes < CONFIG.cooldownMinutes){
      return { send: false, reason: `Cooldown aktiv (${Math.round(ageMinutes)} / ${CONFIG.cooldownMinutes} min).` };
    }
  }

  return { send: true, reason: "Threshold erreicht." };
}

function buildTelegramMessage(signal){
  const tf = Object.fromEntries(signal.timeframes.map((item) => [item.timeframe, item]));
  const trade = signal.trade;

  return [
    "AI Analyse Signal",
    `${signal.symbol} (${signal.providerSymbol})`,
    "",
    `${formatDirection(signal.direction)} ${formatPercent(signal.confidence)} | Qualitaet ${signal.setupQuality}`,
    `Preis: ${formatPrice(signal.lastPrice)} | Session: ${signal.session}`,
    `Score: ${signal.weightedScore.toFixed(2)}`,
    `V4 Gate: ${signal.signalGate && signal.signalGate.canSend ? "FREI" : "BLOCK"} | ${signal.signalGate ? signal.signalGate.summary : "--"}`,
    "",
    `H1: ${tf.H1 ? tf.H1.biasKey : "--"} | M15: ${tf.M15 ? tf.M15.biasKey : "--"} | M5: ${tf.M5 ? tf.M5.biasKey : "--"} | M1: ${tf.M1 ? tf.M1.biasKey : "--"}`,
    `Grund: ${signal.reason}`,
    "",
    `Entry: ${formatPrice(trade.entryLow)} - ${formatPrice(trade.entryHigh)}`,
    `Invalid: ${formatPrice(trade.invalidation)}`,
    `TP1: ${formatPrice(trade.tp1)} | TP2: ${formatPrice(trade.tp2)}`,
    "",
    "Hinweis: V4 Alert nur bei Entry-Touch + A-Setup. Keine Handelsempfehlung. Vor Entry immer Chart, News und Risiko pruefen."
  ].join("\n");
}

async function sendTelegram(message){
  if(!CONFIG.telegramToken || !CONFIG.telegramChatId){
    throw new Error("TELEGRAM_BOT_TOKEN und TELEGRAM_CHAT_ID muessen gesetzt sein.");
  }

  const response = await fetch(`https://api.telegram.org/bot${CONFIG.telegramToken}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: CONFIG.telegramChatId,
      text: message,
      disable_web_page_preview: true
    })
  });

  if(!response.ok){
    const body = await response.text();
    throw new Error(`Telegram sendMessage fehlgeschlagen: HTTP ${response.status} ${body}`);
  }

  return response.json();
}

async function main(){
  const previousState = await readPreviousState();
  if(CONFIG.requireCloudMemory && !isCloudMemoryConfigured()){
    throw new Error("Cloud Memory ist verpflichtend, aber SUPABASE_URL oder SUPABASE_SERVICE_ROLE_KEY fehlt.");
  }

  const signal = await buildSignal(previousState);
  const decision = shouldSendAlert(signal, previousState);
  const message = buildTelegramMessage(signal);
  let cloudOutcomeStatus = {enabled: isCloudMemoryConfigured(), checked: 0, resolved: 0};
  let cloudSignalStatus = {enabled: isCloudMemoryConfigured(), saved: false};

  if(CONFIG.dryRun){
    cloudOutcomeStatus = {
      enabled: isCloudMemoryConfigured(),
      checked: 0,
      resolved: 0,
      dryRun: true
    };
  }else{
    try{
      cloudOutcomeStatus = await resolveCloudSignalOutcomes(signal);
    }catch(error){
      cloudOutcomeStatus = {
        enabled: true,
        checked: 0,
        resolved: 0,
        error: error.message
      };
      console.warn("Cloud Outcome Sync fehlgeschlagen: " + error.message);
    }
  }

  console.log(JSON.stringify({
    symbol: signal.symbol,
    providerSymbol: signal.providerSymbol,
    dataProvider: signal.dataProvider,
    direction: signal.direction,
    confidence: Math.round(signal.confidence),
    threshold: CONFIG.threshold,
    setupQuality: signal.setupQuality,
    signalGate: signal.signalGate ? signal.signalGate.summary : "",
    signalGatePass: Boolean(signal.signalGate && signal.signalGate.canSend),
    decision: decision.reason,
    dryRun: CONFIG.dryRun,
    cloudMemory: cloudOutcomeStatus
  }, null, 2));

  if(decision.send){
    if(CONFIG.dryRun){
      console.log("\n--- SIGNAL DRY RUN ---\n" + message);
    }else{
      try{
        cloudSignalStatus = await saveCloudSignal(signal);
        if(cloudSignalStatus.saved){
          console.log("V4 Signal im Cloud Memory gespeichert.");
        }
      }catch(error){
        cloudSignalStatus = {
          enabled: true,
          saved: false,
          error: error.message
        };
        console.warn("Cloud Signal konnte nicht gespeichert werden: " + error.message);
        if(CONFIG.requireCloudMemory){
          throw error;
        }
      }

      if(isTelegramConfigured()){
        await sendTelegram(message);
        console.log("Telegram Alert gesendet.");
      }else{
        console.log("Telegram ist nicht eingerichtet. Cloud Memory laeuft ohne Benachrichtigung weiter.");
      }
    }

    await writeState({
      lastAlertAt: new Date().toISOString(),
      lastDirection: signal.direction,
      lastConfidence: signal.confidence,
      lastPrice: signal.lastPrice,
      lastReason: signal.reason,
      lastGate: signal.signalGate,
      lastMarketTimeOffsetMs: signal.marketTimeOffsetMs,
      lastMarketTimeOffsetAt: new Date().toISOString(),
      lastCloudOutcomeSync: cloudOutcomeStatus,
      lastCloudSignalSync: cloudSignalStatus
    });
  }else{
    console.log("Kein neues V4 Cloud-Signal: " + decision.reason);
    await writeState({
      ...(previousState || {}),
      lastCheckAt: new Date().toISOString(),
      lastDirectionChecked: signal.direction,
      lastConfidenceChecked: signal.confidence,
      lastPriceChecked: signal.lastPrice,
      lastNoAlertReason: decision.reason,
      lastGateChecked: signal.signalGate,
      lastMarketTimeOffsetMs: signal.marketTimeOffsetMs,
      lastMarketTimeOffsetAt: new Date().toISOString(),
      lastCloudOutcomeSync: cloudOutcomeStatus
    });

    if(CONFIG.failOnNoSignal){
      process.exitCode = 1;
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
