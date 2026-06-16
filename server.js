const express = require("express");
const cors = require("cors");
require("dotenv").config();

const app = express();

app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const DATABENTO_API_KEY = process.env.DATABENTO_API_KEY;

const SYMBOLS = {
  NQ: "NQ.c.0",
  ES: "ES.c.0",
  MNQ: "MNQ.c.0",
  MES: "MES.c.0"
};

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "edgeos-databento-proxy",
    hasKey: !!DATABENTO_API_KEY,
    time: new Date().toISOString()
  });
});

app.get("/snapshot", async (req, res) => {
  try {
    if (!DATABENTO_API_KEY) {
      return res.status(500).json({
        ok: false,
        error: "Missing DATABENTO_API_KEY"
      });
    }

    const results = {};

    for (const [symbol, databentoSymbol] of Object.entries(SYMBOLS)) {
      const startTime = new Date(Date.now() - 30 * 60 * 1000).toISOString();

      const url =
        `https://hist.databento.com/v0/timeseries.get_range` +
        `?dataset=GLBX.MDP3` +
        `&symbols=${encodeURIComponent(databentoSymbol)}` +
        `&schema=trades` +
        `&stype_in=continuous` +
        `&start=${encodeURIComponent(startTime)}` +
        `&encoding=json`;

      const response = await fetch(url, {
        headers: {
          Authorization:
            "Basic " + Buffer.from(DATABENTO_API_KEY + ":").toString("base64")
        }
      });

      const text = await response.text();

      if (!response.ok) {
        results[symbol] = {
          ok: false,
          error: text
        };
        continue;
      }

      const lines = text
        .trim()
        .split("\n")
        .filter(Boolean)
        .map(line => {
          try {
            return JSON.parse(line);
          } catch {
            return null;
          }
        })
        .filter(Boolean);

      const lastTrade = lines[lines.length - 1];

      results[symbol] = {
        ok: !!lastTrade,
        symbol,
        databentoSymbol,
        price: lastTrade?.price ?? null,
        raw: lastTrade ?? null,
        updatedAt: new Date().toISOString()
      };
    }

    res.json({
      ok: true,
      mode: "historical-delayed-context",
      source: "Databento GLBX.MDP3",
      generatedAt: new Date().toISOString(),
      data: results
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`EdgeOS Databento proxy running on port ${PORT}`);
});
