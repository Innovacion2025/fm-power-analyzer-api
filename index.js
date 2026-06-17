// ============================================================
// FM POWER ANALYZER - SERVICIO API (Express + PostgreSQL)
// Fase 0.3: este servicio expone /api/* (REST + CSV).
// El ingest de gateways (/analyzer) y el editor Node-RED (/admin)
// viven en el servicio fm-power-analyzer-server.
// ============================================================

// ============================================================
// BLOQUE 1: IMPORTACIONES
// ============================================================
const crypto = require("crypto");
const fs = require("fs");
const https = require("https");
const path = require("path");
const express = require("express");
const { Pool } = require("pg");
require("dotenv").config();

// ============================================================
// BLOQUE 2: APP BASE
// ============================================================
const app = express();

app.use(express.json());

// ============================================================
// BLOQUE 3: CORS GLOBAL PARA API Y DASHBOARD
// ============================================================
const CORS_ALLOWED_ORIGINS = ["https://innovacion2025.github.io"];

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (origin && CORS_ALLOWED_ORIGINS.includes(origin)) {
    res.header("Access-Control-Allow-Origin", origin);
  }

  res.header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");

  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }

  next();
});

// ============================================================
// BLOQUE 7: CONEXION A POSTGRESQL
// ============================================================
const dbUrl = String(process.env.DATABASE_URL || "").trim();

const dbCaCert = fs.readFileSync(path.join(__dirname, "certs", "supabase-ca.crt"), "utf8");

const pool = new Pool({
  connectionString: dbUrl,
  ssl: { ca: dbCaCert, rejectUnauthorized: true }
});

// ============================================================
// BLOQUE 7A: MEMORIA TEMPORAL DE ULTIMA LECTURA POR MEDIDOR
// ============================================================
const latestReadings = {};

// ============================================================
// BLOQUE 7B: CLAVE INTERNA COMPARTIDA (X-Internal-Key)
// ============================================================
const INTERNAL_API_KEY = String(process.env.INTERNAL_API_KEY || "").trim();

function verificarInternalKey(req) {
  if (!INTERNAL_API_KEY) {
    return false;
  }

  const providedKey = String(req.headers["x-internal-key"] || "");
  const expectedKey = Buffer.from(INTERNAL_API_KEY, "utf8");
  const actualKey = Buffer.from(providedKey, "utf8");

  return (
    expectedKey.length === actualKey.length &&
    crypto.timingSafeEqual(expectedKey, actualKey)
  );
}

function buildMeterKey(deviceId, pmSlave) {
  return `${deviceId}__${pmSlave}`;
}

async function probarPostgres() {
  try {
    const result = await pool.query("SELECT NOW() as fecha");
    console.log("PostgreSQL conectado OK:", result.rows[0].fecha);
  } catch (error) {
    console.error("Error conectando a PostgreSQL:", error);
  }
}

// ============================================================
// BLOQUE 8: CREACION DE TABLAS E INDICES
// ============================================================
async function crearTablasSiNoExisten() {
  try {
    console.log("Iniciando creacion de tablas...");

    await pool.query(`
      CREATE TABLE IF NOT EXISTS power_meters (
        id BIGSERIAL PRIMARY KEY,
        device_id TEXT NOT NULL,
        device_name TEXT,
        pm_slave INTEGER NOT NULL,
        pm_name TEXT,
        model TEXT,
        fw TEXT,
        token TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (device_id, pm_slave)
      );
    `);
    console.log("Tabla power_meters OK");

    await pool.query(`
      CREATE TABLE IF NOT EXISTS power_readings (
        id BIGSERIAL PRIMARY KEY,
        device_id TEXT NOT NULL,
        device_name TEXT,
        pm_slave INTEGER NOT NULL,
        pm_name TEXT,
        token TEXT,
        model TEXT,
        fw TEXT,
        status TEXT,
        uptime_ms BIGINT,
        ip TEXT,
        rssi INTEGER,
        timestamp_ms BIGINT,
        offline_replay BOOLEAN NOT NULL DEFAULT FALSE,
        offline_seq BIGINT,
        sample_group_id TEXT,
        sample_group_index INTEGER,
        sample_group_total INTEGER,
        sample_interval_ms BIGINT,
        voltage_a NUMERIC(12,3),
        voltage_b NUMERIC(12,3),
        voltage_c NUMERIC(12,3),
        current_a NUMERIC(12,3),
        current_b NUMERIC(12,3),
        current_c NUMERIC(12,3),
        current_n NUMERIC(12,3),
        p_a NUMERIC(14,3),
        p_b NUMERIC(14,3),
        p_c NUMERIC(14,3),
        p_tot NUMERIC(14,3),
        q_a NUMERIC(14,3),
        q_b NUMERIC(14,3),
        q_c NUMERIC(14,3),
        q_tot NUMERIC(14,3),
        s_a NUMERIC(14,3),
        s_b NUMERIC(14,3),
        s_c NUMERIC(14,3),
        s_tot NUMERIC(14,3),
        pf_a NUMERIC(12,3),
        pf_b NUMERIC(12,3),
        pf_c NUMERIC(12,3),
        pf_tot NUMERIC(12,3),
        frecuencia NUMERIC(12,3),
        active_energy NUMERIC(14,3),
        thd_va NUMERIC(12,3),
        thd_vb NUMERIC(12,3),
        thd_vc NUMERIC(12,3),
        thd_ia NUMERIC(12,3),
        thd_ib NUMERIC(12,3),
        thd_ic NUMERIC(12,3),
        thd_in NUMERIC(12,3),
        desbalance_v NUMERIC(12,3),
        desbalance_i NUMERIC(12,3),
        raw_payload JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    console.log("Tabla power_readings OK");

    await pool.query(`
      CREATE TABLE IF NOT EXISTS power_latest (
        id BIGSERIAL PRIMARY KEY,
        device_id TEXT NOT NULL,
        device_name TEXT,
        pm_slave INTEGER NOT NULL,
        pm_name TEXT,
        token TEXT,
        model TEXT,
        fw TEXT,
        status TEXT,
        uptime_ms BIGINT,
        ip TEXT,
        rssi INTEGER,
        timestamp_ms BIGINT,
        offline_replay BOOLEAN NOT NULL DEFAULT FALSE,
        offline_seq BIGINT,
        sample_group_id TEXT,
        sample_group_index INTEGER,
        sample_group_total INTEGER,
        sample_interval_ms BIGINT,
        voltage_a NUMERIC(12,3),
        voltage_b NUMERIC(12,3),
        voltage_c NUMERIC(12,3),
        current_a NUMERIC(12,3),
        current_b NUMERIC(12,3),
        current_c NUMERIC(12,3),
        current_n NUMERIC(12,3),
        p_a NUMERIC(14,3),
        p_b NUMERIC(14,3),
        p_c NUMERIC(14,3),
        p_tot NUMERIC(14,3),
        q_a NUMERIC(14,3),
        q_b NUMERIC(14,3),
        q_c NUMERIC(14,3),
        q_tot NUMERIC(14,3),
        s_a NUMERIC(14,3),
        s_b NUMERIC(14,3),
        s_c NUMERIC(14,3),
        s_tot NUMERIC(14,3),
        pf_a NUMERIC(12,3),
        pf_b NUMERIC(12,3),
        pf_c NUMERIC(12,3),
        pf_tot NUMERIC(12,3),
        frecuencia NUMERIC(12,3),
        active_energy NUMERIC(14,3),
        thd_va NUMERIC(12,3),
        thd_vb NUMERIC(12,3),
        thd_vc NUMERIC(12,3),
        thd_ia NUMERIC(12,3),
        thd_ib NUMERIC(12,3),
        thd_ic NUMERIC(12,3),
        thd_in NUMERIC(12,3),
        desbalance_v NUMERIC(12,3),
        desbalance_i NUMERIC(12,3),
        raw_payload JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (device_id, pm_slave)
      );
    `);
    console.log("Tabla power_latest OK");

    await pool.query(`
      ALTER TABLE power_readings
      ADD COLUMN IF NOT EXISTS active_energy NUMERIC(14,3);
    `);
    console.log("Columna active_energy en power_readings OK");

    await pool.query(`
      ALTER TABLE power_latest
      ADD COLUMN IF NOT EXISTS active_energy NUMERIC(14,3);
    `);
    console.log("Columna active_energy en power_latest OK");

    await pool.query(`
      ALTER TABLE power_readings
      ADD COLUMN IF NOT EXISTS offline_replay BOOLEAN NOT NULL DEFAULT FALSE,
      ADD COLUMN IF NOT EXISTS offline_seq BIGINT,
      ADD COLUMN IF NOT EXISTS sample_group_id TEXT,
      ADD COLUMN IF NOT EXISTS sample_group_index INTEGER,
      ADD COLUMN IF NOT EXISTS sample_group_total INTEGER,
      ADD COLUMN IF NOT EXISTS sample_interval_ms BIGINT;
    `);
    console.log("Campos offline/store-forward en power_readings OK");

    await pool.query(`
      ALTER TABLE power_latest
      ADD COLUMN IF NOT EXISTS offline_replay BOOLEAN NOT NULL DEFAULT FALSE,
      ADD COLUMN IF NOT EXISTS offline_seq BIGINT,
      ADD COLUMN IF NOT EXISTS sample_group_id TEXT,
      ADD COLUMN IF NOT EXISTS sample_group_index INTEGER,
      ADD COLUMN IF NOT EXISTS sample_group_total INTEGER,
      ADD COLUMN IF NOT EXISTS sample_interval_ms BIGINT;
    `);
    console.log("Campos offline/store-forward en power_latest OK");

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_power_readings_device_slave_time
      ON power_readings (device_id, pm_slave, created_at DESC);
    `);
    console.log("Indice 1 OK");

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_power_readings_device_slave
      ON power_readings (device_id, pm_slave);
    `);
    console.log("Indice 2 OK");

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_power_latest_device_slave
      ON power_latest (device_id, pm_slave);
    `);
    console.log("Indice 3 OK");

    console.log("Tablas power_meters, power_readings y power_latest listas");
  } catch (error) {
    console.error("Error creando tablas:", error);
  }
}

// ============================================================
// BLOQUE 8A: REGISTRAR MEDIDOR LOGICO
// ============================================================
async function upsertMeter(data) {
  const sql = `
    INSERT INTO power_meters (
      device_id,
      device_name,
      pm_slave,
      pm_name,
      model,
      fw,
      token,
      updated_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
    ON CONFLICT (device_id, pm_slave)
    DO UPDATE SET
      device_name = EXCLUDED.device_name,
      pm_name = EXCLUDED.pm_name,
      model = EXCLUDED.model,
      fw = EXCLUDED.fw,
      token = EXCLUDED.token,
      updated_at = NOW()
  `;

  const values = [
    data.device_id,
    data.device_name ?? null,
    data.pm_slave,
    data.pm_name ?? null,
    data.model ?? null,
    data.fw ?? null,
    data.token ?? null
  ];

  await pool.query(sql, values);
}

// ============================================================
// BLOQUE 8B: ACTUALIZAR ULTIMO VALOR EN DB
// ============================================================
async function upsertLatest(data) {
  const p = data.payload || {};
  const offlineReplay = data.offline_replay === true;
  const offlineSeq = data.offline_seq ?? null;
  const sampleGroupId = data.sample_group_id ?? null;
  const sampleGroupIndex = data.sample_group_index ?? null;
  const sampleGroupTotal = data.sample_group_total ?? null;
  const sampleIntervalMs = data.sample_interval_ms ?? null;

  const sql = `
    INSERT INTO power_latest (
      device_id,
      device_name,
      pm_slave,
      pm_name,
      token,
      model,
      fw,
      status,
      uptime_ms,
      ip,
      rssi,
      timestamp_ms,
      offline_replay,
      offline_seq,
      sample_group_id,
      sample_group_index,
      sample_group_total,
      sample_interval_ms,

      voltage_a,
      voltage_b,
      voltage_c,

      current_a,
      current_b,
      current_c,
      current_n,

      p_a,
      p_b,
      p_c,
      p_tot,

      q_a,
      q_b,
      q_c,
      q_tot,

      s_a,
      s_b,
      s_c,
      s_tot,

      pf_a,
      pf_b,
      pf_c,
      pf_tot,

      frecuencia,
      active_energy,

      thd_va,
      thd_vb,
      thd_vc,

      thd_ia,
      thd_ib,
      thd_ic,
      thd_in,

      desbalance_v,
      desbalance_i,

      raw_payload,
      created_at,
      updated_at
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
      $13,$14,$15,$16,$17,$18,
      $19,$20,$21,
      $22,$23,$24,$25,
      $26,$27,$28,$29,
      $30,$31,$32,$33,
      $34,$35,$36,$37,
      $38,$39,$40,$41,
      $42,$43,
      $44,$45,$46,
      $47,$48,$49,$50,
      $51,$52,
      $53,
      NOW(),
      NOW()
    )
    ON CONFLICT (device_id, pm_slave)
    DO UPDATE SET
      device_name = EXCLUDED.device_name,
      pm_name = EXCLUDED.pm_name,
      token = EXCLUDED.token,
      model = EXCLUDED.model,
      fw = EXCLUDED.fw,
      status = EXCLUDED.status,
      uptime_ms = EXCLUDED.uptime_ms,
      ip = EXCLUDED.ip,
      rssi = EXCLUDED.rssi,
      timestamp_ms = EXCLUDED.timestamp_ms,
      offline_replay = EXCLUDED.offline_replay,
      offline_seq = EXCLUDED.offline_seq,
      sample_group_id = EXCLUDED.sample_group_id,
      sample_group_index = EXCLUDED.sample_group_index,
      sample_group_total = EXCLUDED.sample_group_total,
      sample_interval_ms = EXCLUDED.sample_interval_ms,

      voltage_a = EXCLUDED.voltage_a,
      voltage_b = EXCLUDED.voltage_b,
      voltage_c = EXCLUDED.voltage_c,

      current_a = EXCLUDED.current_a,
      current_b = EXCLUDED.current_b,
      current_c = EXCLUDED.current_c,
      current_n = EXCLUDED.current_n,

      p_a = EXCLUDED.p_a,
      p_b = EXCLUDED.p_b,
      p_c = EXCLUDED.p_c,
      p_tot = EXCLUDED.p_tot,

      q_a = EXCLUDED.q_a,
      q_b = EXCLUDED.q_b,
      q_c = EXCLUDED.q_c,
      q_tot = EXCLUDED.q_tot,

      s_a = EXCLUDED.s_a,
      s_b = EXCLUDED.s_b,
      s_c = EXCLUDED.s_c,
      s_tot = EXCLUDED.s_tot,

      pf_a = EXCLUDED.pf_a,
      pf_b = EXCLUDED.pf_b,
      pf_c = EXCLUDED.pf_c,
      pf_tot = EXCLUDED.pf_tot,

      frecuencia = EXCLUDED.frecuencia,
      active_energy = EXCLUDED.active_energy,

      thd_va = EXCLUDED.thd_va,
      thd_vb = EXCLUDED.thd_vb,
      thd_vc = EXCLUDED.thd_vc,

      thd_ia = EXCLUDED.thd_ia,
      thd_ib = EXCLUDED.thd_ib,
      thd_ic = EXCLUDED.thd_ic,
      thd_in = EXCLUDED.thd_in,

      desbalance_v = EXCLUDED.desbalance_v,
      desbalance_i = EXCLUDED.desbalance_i,

      raw_payload = EXCLUDED.raw_payload,
      updated_at = NOW()
    RETURNING id, updated_at
  `;

  const values = [
    data.device_id,
    data.device_name ?? null,
    data.pm_slave,
    data.pm_name ?? null,
    data.token ?? null,
    data.model ?? null,
    data.fw ?? null,
    data.status ?? null,
    data.uptime_ms ?? null,
    data.ip ?? null,
    data.rssi ?? null,
    data.timestamp_ms ?? null,

    offlineReplay,
    offlineSeq,
    sampleGroupId,
    sampleGroupIndex,
    sampleGroupTotal,
    sampleIntervalMs,

    p.voltage_a ?? null,
    p.voltage_b ?? null,
    p.voltage_c ?? null,

    p.current_a ?? null,
    p.current_b ?? null,
    p.current_c ?? null,
    p.current_n ?? null,

    p.p_a ?? null,
    p.p_b ?? null,
    p.p_c ?? null,
    p.p_tot ?? null,

    p.q_a ?? null,
    p.q_b ?? null,
    p.q_c ?? null,
    p.q_tot ?? null,

    p.s_a ?? null,
    p.s_b ?? null,
    p.s_c ?? null,
    p.s_tot ?? null,

    p.pf_a ?? null,
    p.pf_b ?? null,
    p.pf_c ?? null,
    p.pf_tot ?? null,

    p.frecuencia ?? null,
    p.active_energy ?? null,

    p.thd_va ?? null,
    p.thd_vb ?? null,
    p.thd_vc ?? null,

    p.thd_ia ?? null,
    p.thd_ib ?? null,
    p.thd_ic ?? null,
    p.thd_in ?? null,

    p.desbalance_v ?? null,
    p.desbalance_i ?? null,

    data
  ];

  return pool.query(sql, values);
}

// ============================================================
// BLOQUE 8C: CREACION DE TABLAS PEAK
// ============================================================
async function crearTablasPeakSiNoExisten() {
  try {
    console.log("Iniciando creacion de tablas PEAK...");

    await pool.query(`
      CREATE TABLE IF NOT EXISTS peak_devices (
        id BIGSERIAL PRIMARY KEY,
        device_id TEXT NOT NULL UNIQUE,
        device_name TEXT,
        token TEXT,
        model TEXT,
        fw TEXT,
        conn_mode TEXT,
        ip TEXT,
        rssi INTEGER,
        uptime_ms BIGINT,
        device_timestamp_ms BIGINT,
        raw_payload JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    console.log("Tabla peak_devices OK");

    await pool.query(`
      CREATE TABLE IF NOT EXISTS peak_counters (
        id BIGSERIAL PRIMARY KEY,
        device_id TEXT NOT NULL,
        counter_index INTEGER NOT NULL,
        slave_id INTEGER,
        counter_name TEXT,
        counter_type TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (device_id, counter_index)
      );
    `);
    console.log("Tabla peak_counters OK");

    await pool.query(`
      CREATE TABLE IF NOT EXISTS peak_readings (
        id BIGSERIAL PRIMARY KEY,
        device_id TEXT NOT NULL,
        device_name TEXT,
        token TEXT,
        model TEXT,
        fw TEXT,
        conn_mode TEXT,
        ip TEXT,
        rssi INTEGER,
        uptime_ms BIGINT,
        device_timestamp_ms BIGINT,

        counter_index INTEGER NOT NULL,
        slave_id INTEGER,
        counter_name TEXT,
        counter_type TEXT,
        counter_value BIGINT,
        online BOOLEAN,
        fail_count INTEGER,

        raw_payload JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    console.log("Tabla peak_readings OK");

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_peak_counters_device
      ON peak_counters (device_id, counter_index);
    `);
    console.log("Indice peak_counters OK");

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_peak_readings_device_counter_time
      ON peak_readings (device_id, counter_index, created_at DESC);
    `);
    console.log("Indice peak_readings tiempo OK");

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_peak_readings_device_counter
      ON peak_readings (device_id, counter_index);
    `);
    console.log("Indice peak_readings device_counter OK");

    console.log("Tablas PEAK listas");
  } catch (error) {
    console.error("Error creando tablas PEAK:", error);
  }
}

// ============================================================
// BLOQUE 8D: UPSERT DISPOSITIVO PEAK
// ============================================================
async function upsertPeakDevice(data) {
  const sql = `
    INSERT INTO peak_devices (
      device_id,
      device_name,
      token,
      model,
      fw,
      conn_mode,
      ip,
      rssi,
      uptime_ms,
      device_timestamp_ms,
      raw_payload,
      updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
    ON CONFLICT (device_id)
    DO UPDATE SET
      device_name = EXCLUDED.device_name,
      token = EXCLUDED.token,
      model = EXCLUDED.model,
      fw = EXCLUDED.fw,
      conn_mode = EXCLUDED.conn_mode,
      ip = EXCLUDED.ip,
      rssi = EXCLUDED.rssi,
      uptime_ms = EXCLUDED.uptime_ms,
      device_timestamp_ms = EXCLUDED.device_timestamp_ms,
      raw_payload = EXCLUDED.raw_payload,
      updated_at = NOW()
  `;

  const values = [
    data.device_id,
    data.device_name ?? null,
    data.token ?? null,
    data.model ?? null,
    data.fw ?? null,
    data.conn_mode ?? null,
    data.ip ?? null,
    data.rssi ?? null,
    data.uptime_ms ?? null,
    data.device_timestamp_ms ?? null,
    data
  ];

  await pool.query(sql, values);
}

// ============================================================
// BLOQUE 8E: UPSERT CONTADOR PEAK
// ============================================================
async function upsertPeakCounter(deviceId, counter) {
  const sql = `
    INSERT INTO peak_counters (
      device_id,
      counter_index,
      slave_id,
      counter_name,
      counter_type,
      updated_at
    )
    VALUES ($1,$2,$3,$4,$5,NOW())
    ON CONFLICT (device_id, counter_index)
    DO UPDATE SET
      slave_id = EXCLUDED.slave_id,
      counter_name = EXCLUDED.counter_name,
      counter_type = EXCLUDED.counter_type,
      updated_at = NOW()
  `;

  const values = [
    deviceId,
    counter.index,
    counter.slave_id ?? null,
    counter.name ?? null,
    counter.type ?? null
  ];

  await pool.query(sql, values);
}

// ============================================================
// BLOQUE 8F: INSERT HISTORICO PEAK
// ============================================================
async function insertPeakReading(data, counter) {
  const sql = `
    INSERT INTO peak_readings (
      device_id,
      device_name,
      token,
      model,
      fw,
      conn_mode,
      ip,
      rssi,
      uptime_ms,
      device_timestamp_ms,

      counter_index,
      slave_id,
      counter_name,
      counter_type,
      counter_value,
      online,
      fail_count,

      raw_payload
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
      $11,$12,$13,$14,$15,$16,$17,
      $18
    )
    RETURNING id, created_at
  `;

  const values = [
    data.device_id,
    data.device_name ?? null,
    data.token ?? null,
    data.model ?? null,
    data.fw ?? null,
    data.conn_mode ?? null,
    data.ip ?? null,
    data.rssi ?? null,
    data.uptime_ms ?? null,
    data.device_timestamp_ms ?? null,

    counter.index,
    counter.slave_id ?? null,
    counter.name ?? null,
    counter.type ?? null,
    counter.value ?? null,
    counter.online ?? null,
    counter.fail_count ?? null,

    data
  ];

  return pool.query(sql, values);
}

// ============================================================
// BLOQUE 10: API - GUARDAR LECTURA
// ============================================================
app.post("/api/save-reading", async (req, res) => {
  try {
    if (!verificarInternalKey(req)) {
      return res.status(403).json({ ok: false, error: "forbidden" });
    }

    const data = req.body;
    const p = data.payload || {};

    if (!data.device_id) {
      return res.status(400).json({ ok: false, error: "Falta device_id" });
    }

    if (data.pm_slave === undefined || data.pm_slave === null) {
      return res.status(400).json({ ok: false, error: "Falta pm_slave" });
    }

    const pmSlave = Number(data.pm_slave);

    if (!Number.isInteger(pmSlave)) {
      return res.status(400).json({ ok: false, error: "pm_slave debe ser entero" });
    }

    data.pm_slave = pmSlave;

    const meterKey = buildMeterKey(data.device_id, data.pm_slave);

    latestReadings[meterKey] = {
      data,
      updated_at: new Date().toISOString()
    };

    await upsertMeter(data);
    await upsertLatest(data);

    // Evaluar alarmas del device en tiempo real con el payload recién recibido.
    // No bloquea la respuesta al gateway (fire-and-forget con captura de errores).
    evaluarAlarmasParaDevice(data.device_id, data.pm_slave, data.payload || {}).catch(() => {});

    const SAVE_INTERVAL = 2000;

    if (!global.lastSaveTimes) {
      global.lastSaveTimes = {};
    }

    const now = Date.now();
    const lastSave = global.lastSaveTimes[meterKey] || 0;

    if (now - lastSave < SAVE_INTERVAL) {
      return res.json({
        ok: true,
        saved_to_db: false,
        updated_latest: true,
        device_id: data.device_id,
        device_name: data.device_name ?? null,
        pm_slave: data.pm_slave,
        pm_name: data.pm_name ?? null,
        message: "Dato recibido, ultimo valor actualizado"
      });
    }

    global.lastSaveTimes[meterKey] = now;

    const sql = `
      INSERT INTO power_readings (
        device_id,
        device_name,
        pm_slave,
        pm_name,
        token,
        model,
        fw,
        status,
        uptime_ms,
        ip,
        rssi,
        timestamp_ms,
        offline_replay,
        offline_seq,
        sample_group_id,
        sample_group_index,
        sample_group_total,
        sample_interval_ms,

        voltage_a,
        voltage_b,
        voltage_c,

        current_a,
        current_b,
        current_c,
        current_n,

        p_a,
        p_b,
        p_c,
        p_tot,

        q_a,
        q_b,
        q_c,
        q_tot,

        s_a,
        s_b,
        s_c,
        s_tot,

        pf_a,
        pf_b,
        pf_c,
        pf_tot,

        frecuencia,
        active_energy,

        thd_va,
        thd_vb,
        thd_vc,

        thd_ia,
        thd_ib,
        thd_ic,
        thd_in,

        desbalance_v,
        desbalance_i,

        raw_payload
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
        $13,$14,$15,$16,$17,$18,
        $19,$20,$21,
        $22,$23,$24,$25,
        $26,$27,$28,$29,
        $30,$31,$32,$33,
        $34,$35,$36,$37,
        $38,$39,$40,$41,
        $42,$43,
        $44,$45,$46,
        $47,$48,$49,$50,
        $51,$52,
        $53
      )
      RETURNING id, created_at
    `;

    const offlineReplay = data.offline_replay === true;
    const offlineSeq = data.offline_seq ?? null;
    const sampleGroupId = data.sample_group_id ?? null;
    const sampleGroupIndex = data.sample_group_index ?? null;
    const sampleGroupTotal = data.sample_group_total ?? null;
    const sampleIntervalMs = data.sample_interval_ms ?? null;

    const values = [
      data.device_id,
      data.device_name ?? null,
      data.pm_slave,
      data.pm_name ?? null,
      data.token ?? null,
      data.model ?? null,
      data.fw ?? null,
      data.status ?? null,
      data.uptime_ms ?? null,
      data.ip ?? null,
      data.rssi ?? null,
      data.timestamp_ms ?? null,

      offlineReplay,
      offlineSeq,
      sampleGroupId,
      sampleGroupIndex,
      sampleGroupTotal,
      sampleIntervalMs,

      p.voltage_a ?? null,
      p.voltage_b ?? null,
      p.voltage_c ?? null,

      p.current_a ?? null,
      p.current_b ?? null,
      p.current_c ?? null,
      p.current_n ?? null,

      p.p_a ?? null,
      p.p_b ?? null,
      p.p_c ?? null,
      p.p_tot ?? null,

      p.q_a ?? null,
      p.q_b ?? null,
      p.q_c ?? null,
      p.q_tot ?? null,

      p.s_a ?? null,
      p.s_b ?? null,
      p.s_c ?? null,
      p.s_tot ?? null,

      p.pf_a ?? null,
      p.pf_b ?? null,
      p.pf_c ?? null,
      p.pf_tot ?? null,

      p.frecuencia ?? null,
      p.active_energy ?? null,

      p.thd_va ?? null,
      p.thd_vb ?? null,
      p.thd_vc ?? null,

      p.thd_ia ?? null,
      p.thd_ib ?? null,
      p.thd_ic ?? null,
      p.thd_in ?? null,

      p.desbalance_v ?? null,
      p.desbalance_i ?? null,

      data
    ];

    const result = await pool.query(sql, values);

    return res.json({
      ok: true,
      saved_to_db: true,
      updated_latest: true,
      id: result.rows[0].id,
      created_at: result.rows[0].created_at,
      device_id: data.device_id,
      device_name: data.device_name ?? null,
      pm_slave: data.pm_slave,
      pm_name: data.pm_name ?? null
    });

  } catch (error) {
    console.error("Error guardando lectura:", error);
    return res.status(500).json({
      ok: false,
      error: "Error guardando lectura",
      detail: error.message
    });
  }
});


// ============================================================
// BLOQUE 11: API - ULTIMA LECTURA DE UN PM EN TIEMPO REAL
// ============================================================
app.get("/api/device/:device_id", async (req, res) => {
  try {
    const { device_id } = req.params;
    const pmSlave = Number(req.query.pm_slave);

    if (!Number.isInteger(pmSlave)) {
      return res.status(400).json({
        ok: false,
        error: "Falta pm_slave valido en query"
      });
    }

    const sql = `
      SELECT
        l.device_id,
        m.device_name AS device_name,
        l.pm_slave,
        m.pm_name AS pm_name,

        l.token,
        l.model,
        l.fw,
        l.status,
        l.uptime_ms,
        l.ip,
        l.rssi,
        l.timestamp_ms,

        l.voltage_a,
        l.voltage_b,
        l.voltage_c,

        l.current_a,
        l.current_b,
        l.current_c,
        l.current_n,

        l.p_a,
        l.p_b,
        l.p_c,
        l.p_tot,

        l.q_a,
        l.q_b,
        l.q_c,
        l.q_tot,

        l.s_a,
        l.s_b,
        l.s_c,
        l.s_tot,

        l.pf_a,
        l.pf_b,
        l.pf_c,
        l.pf_tot,

        l.frecuencia,
        l.active_energy,

        l.thd_va,
        l.thd_vb,
        l.thd_vc,

        l.thd_ia,
        l.thd_ib,
        l.thd_ic,
        l.thd_in,

        l.desbalance_v,
        l.desbalance_i,

        l.raw_payload,
        l.created_at,
        l.updated_at,
        l.updated_at AS visible_at

      FROM power_latest l
      LEFT JOIN power_meters m
        ON l.device_id = m.device_id
       AND l.pm_slave = m.pm_slave
      WHERE l.device_id = $1
        AND l.pm_slave = $2
      LIMIT 1
    `;

    const result = await pool.query(sql, [device_id, pmSlave]);

    if (result.rows.length === 0) {
      return res.json({
        ok: false,
        error: "No se encontraron datos del medidor",
        device_id,
        pm_slave: pmSlave
      });
    }

    res.json({
      ok: true,
      device_id,
      pm_slave: pmSlave,
      data: result.rows[0]
    });
  } catch (error) {
    console.error("Error consultando device:", error.message);
    res.status(500).json({
      ok: false,
      error: "Error consultando device",
      detail: error.message
    });
  }
});

// ============================================================
// BLOQUE 11A: API - LISTAR MEDIDORES DE UN DEVICE
// ============================================================
app.get("/api/meters/:device_id", async (req, res) => {
  try {
    const { device_id } = req.params;

    const result = await pool.query(
      `
      SELECT
        device_id,
        device_name,
        pm_slave,
        pm_name,
        model,
        fw,
        token,
        created_at,
        updated_at
      FROM power_meters
      WHERE device_id = $1
      ORDER BY pm_slave ASC
      `,
      [device_id]
    );

    res.json({
      ok: true,
      device_id,
      total: result.rows.length,
      data: result.rows
    });
  } catch (error) {
    console.error("Error consultando medidores:", error.message);
    res.status(500).json({
      ok: false,
      error: "Error consultando medidores"
    });
  }
});

// ============================================================
// BLOQUE 12: API - HISTORICO PARA GRAFICAS
// ============================================================
app.get("/api/history", async (req, res) => {
  try {
    const { device_id, from, to } = req.query;
    const pmSlave = Number(req.query.pm_slave);

    if (!device_id) {
      return res.status(400).json({ ok: false, error: "Falta device_id" });
    }

    if (!Number.isInteger(pmSlave)) {
      return res.status(400).json({ ok: false, error: "Falta pm_slave valido" });
    }

    // Si no se especifica from/to, usar el día actual para evitar cargar todo el histórico
    const now = new Date();
    const today = now.toISOString().split('T')[0]; // YYYY-MM-DD
    const actualFrom = from || today;
    const actualTo = to || today;

    const sql = `
      SELECT
        r.id,
        r.device_id,
        m.device_name AS device_name,
        r.pm_slave,
        m.pm_name AS pm_name,

        r.timestamp_ms,

        r.voltage_a,
        r.voltage_b,
        r.voltage_c,

        r.current_a,
        r.current_b,
        r.current_c,
        r.current_n,

        r.p_a,
        r.p_b,
        r.p_c,
        r.p_tot,

        r.q_a,
        r.q_b,
        r.q_c,
        r.q_tot,

        r.s_a,
        r.s_b,
        r.s_c,
        r.s_tot,

        r.pf_a,
        r.pf_b,
        r.pf_c,
        r.pf_tot,

        r.frecuencia,
        r.active_energy,

        r.thd_va,
        r.thd_vb,
        r.thd_vc,

        r.thd_ia,
        r.thd_ib,
        r.thd_ic,
        r.thd_in,

        r.desbalance_v,
        r.desbalance_i,

        r.created_at,
        r.created_at AS visible_at
      FROM power_readings r
      LEFT JOIN power_meters m
        ON r.device_id = m.device_id
       AND r.pm_slave = m.pm_slave
      WHERE r.device_id = $1
        AND r.pm_slave = $2
        AND r.created_at >= (($3::date)::timestamp AT TIME ZONE 'America/Guayaquil')
        AND r.created_at < (((($4::date) + INTERVAL '1 day')::timestamp) AT TIME ZONE 'America/Guayaquil')
      ORDER BY r.created_at ASC, r.id ASC
    `;

    const values = [device_id, pmSlave, actualFrom, actualTo];

    const result = await pool.query(sql, values);

    res.json({
      ok: true,
      device_id,
      pm_slave: pmSlave,
      from: actualFrom,
      to: actualTo,
      total: result.rows.length,
      data: result.rows
    });
  } catch (error) {
    console.error("Error consultando histórico:", error);
    res.status(500).json({
      ok: false,
      error: "Error consultando histórico",
      detail: error.message
    });
  }
});

// ============================================================
// BLOQUE 12B: API - HISTORICO INCREMENTAL POR RANGO
// ============================================================
app.get("/api/history/since", async (req, res) => {
  try {
    const { device_id, from, to, last_created_at } = req.query;
    const pmSlave = Number(req.query.pm_slave);
    const lastId = req.query.last_id ? Number(req.query.last_id) : null;

    if (!device_id) {
      return res.status(400).json({ ok: false, error: "Falta device_id" });
    }

    if (!Number.isInteger(pmSlave)) {
      return res.status(400).json({ ok: false, error: "Falta pm_slave valido" });
    }

    if (!from || !to) {
      return res.status(400).json({ ok: false, error: "Faltan fechas from y to" });
    }

    if (!last_created_at && !Number.isInteger(lastId)) {
      return res.status(400).json({
        ok: false,
        error: "Falta last_created_at o last_id"
      });
    }

    const sqlBase = `
      SELECT
        r.id,
        r.device_id,
        m.device_name AS device_name,
        r.pm_slave,
        m.pm_name AS pm_name,

        r.timestamp_ms,

        r.voltage_a,
        r.voltage_b,
        r.voltage_c,

        r.current_a,
        r.current_b,
        r.current_c,
        r.current_n,

        r.p_a,
        r.p_b,
        r.p_c,
        r.p_tot,

        r.q_a,
        r.q_b,
        r.q_c,
        r.q_tot,

        r.s_a,
        r.s_b,
        r.s_c,
        r.s_tot,

        r.pf_a,
        r.pf_b,
        r.pf_c,
        r.pf_tot,

        r.frecuencia,
        r.active_energy,

        r.thd_va,
        r.thd_vb,
        r.thd_vc,

        r.thd_ia,
        r.thd_ib,
        r.thd_ic,
        r.thd_in,

        r.desbalance_v,
        r.desbalance_i,

        r.created_at,
        r.created_at AS visible_at
      FROM power_readings r
      LEFT JOIN power_meters m
        ON r.device_id = m.device_id
       AND r.pm_slave = m.pm_slave
      WHERE r.device_id = $1
        AND r.pm_slave = $2
        AND r.created_at >= (($3::date)::timestamp AT TIME ZONE 'America/Guayaquil')
        AND r.created_at < (((($4::date) + INTERVAL '1 day')::timestamp) AT TIME ZONE 'America/Guayaquil')
    `;

    const values = [device_id, pmSlave, from, to];
    let sql = sqlBase;

    if (last_created_at && Number.isInteger(lastId)) {
      const lastCreatedAtDate = new Date(last_created_at);
      if (isNaN(lastCreatedAtDate.getTime())) {
        return res.status(400).json({
          ok: false,
          error: "last_created_at no es un timestamp ISO valido"
        });
      }

      sql += `
        AND (
          r.created_at > $5
          OR (r.created_at = $5 AND r.id > $6)
        )
      `;
      values.push(last_created_at, lastId);
    } else if (last_created_at) {
      const lastCreatedAtDate = new Date(last_created_at);
      if (isNaN(lastCreatedAtDate.getTime())) {
        return res.status(400).json({
          ok: false,
          error: "last_created_at no es un timestamp ISO valido"
        });
      }

      sql += ` AND r.created_at > $5 `;
      values.push(last_created_at);
    } else {
      sql += ` AND r.id > $5 `;
      values.push(lastId);
    }

    sql += ` ORDER BY r.created_at ASC, r.id ASC LIMIT 1000`;

    const result = await pool.query(sql, values);

    return res.json({
      ok: true,
      device_id,
      pm_slave: pmSlave,
      from,
      to,
      last_created_at: last_created_at || null,
      last_id: Number.isInteger(lastId) ? lastId : null,
      total: result.rows.length,
      data: result.rows
    });
  } catch (error) {
    console.error("Error consultando histórico incremental (/api/history/since):", error);
    res.status(500).json({
      ok: false,
      error: "Error consultando histórico incremental",
      detail: error.message
    });
  }
});

// ============================================================
// BLOQUE 12C: API - HISTORICO INCREMENTAL PARA ACTUALIZACIONES EN TIEMPO REAL
// ============================================================
app.get("/api/history/incremental", async (req, res) => {
  try {
    const { device_id, since } = req.query;
    const pmSlave = Number(req.query.pm_slave);

    if (!device_id) {
      return res.status(400).json({ ok: false, error: "Falta device_id" });
    }

    if (!Number.isInteger(pmSlave)) {
      return res.status(400).json({ ok: false, error: "Falta pm_slave valido" });
    }

    if (!since) {
      return res.status(400).json({ ok: false, error: "Falta parametro since (timestamp ISO)" });
    }

    // Validar que since sea un timestamp ISO valido
    const sinceDate = new Date(since);
    if (isNaN(sinceDate.getTime())) {
      return res.status(400).json({ ok: false, error: "Parametro since debe ser timestamp ISO valido" });
    }

    const sql = `
      SELECT
        r.id,
        r.device_id,
        m.device_name AS device_name,
        r.pm_slave,
        m.pm_name AS pm_name,

        r.timestamp_ms,

        r.voltage_a,
        r.voltage_b,
        r.voltage_c,

        r.current_a,
        r.current_b,
        r.current_c,
        r.current_n,

        r.p_a,
        r.p_b,
        r.p_c,
        r.p_tot,

        r.q_a,
        r.q_b,
        r.q_c,
        r.q_tot,

        r.s_a,
        r.s_b,
        r.s_c,
        r.s_tot,

        r.pf_a,
        r.pf_b,
        r.pf_c,
        r.pf_tot,

        r.frecuencia,
        r.active_energy,

        r.thd_va,
        r.thd_vb,
        r.thd_vc,

        r.thd_ia,
        r.thd_ib,
        r.thd_ic,
        r.thd_in,

        r.desbalance_v,
        r.desbalance_i,

        r.raw_payload,
        r.created_at,
        r.created_at AS visible_at
      FROM power_readings r
      LEFT JOIN power_meters m
        ON r.device_id = m.device_id
       AND r.pm_slave = m.pm_slave
      WHERE r.device_id = $1
        AND r.pm_slave = $2
        AND r.created_at > $3
      ORDER BY r.created_at ASC, r.id ASC
      LIMIT 1000
    `;

    const values = [device_id, pmSlave, since];

    const result = await pool.query(sql, values);

    res.json({
      ok: true,
      device_id,
      pm_slave: pmSlave,
      since,
      total: result.rows.length,
      data: result.rows
    });
  } catch (error) {
    console.error("Error consultando histórico incremental:", error);
    res.status(500).json({
      ok: false,
      error: "Error consultando histórico incremental",
      detail: error.message
    });
  }
});

// ============================================================
// BLOQUE 12D: API - OBTENER ULTIMO TIMESTAMP PARA INCREMENTAL
// ============================================================
app.get("/api/history/latest-timestamp", async (req, res) => {
  try {
    const { device_id } = req.query;
    const pmSlave = Number(req.query.pm_slave);

    if (!device_id) {
      return res.status(400).json({ ok: false, error: "Falta device_id" });
    }

    if (!Number.isInteger(pmSlave)) {
      return res.status(400).json({ ok: false, error: "Falta pm_slave valido" });
    }

    const sql = `
      SELECT MAX(created_at) as latest_timestamp
      FROM power_readings
      WHERE device_id = $1 AND pm_slave = $2
    `;

    const values = [device_id, pmSlave];
    const result = await pool.query(sql, values);

    const latest = result.rows[0]?.latest_timestamp || null;

    res.json({
      ok: true,
      device_id,
      pm_slave: pmSlave,
      latest_timestamp: latest
    });
  } catch (error) {
    console.error("Error obteniendo último timestamp:", error);
    res.status(500).json({
      ok: false,
      error: "Error obteniendo último timestamp",
      detail: error.message
    });
  }
});

// ============================================================
// BLOQUE 12A: API - EXPORTAR HISTORICO EN CSV POR RANGO
// ============================================================
app.get("/api/history/export", async (req, res) => {
  try {
    const { device_id, from, to } = req.query;
    const pmSlave = Number(req.query.pm_slave);

    if (!device_id) {
      return res.status(400).json({ ok: false, error: "Falta device_id" });
    }

    if (!Number.isInteger(pmSlave)) {
      return res.status(400).json({ ok: false, error: "Falta pm_slave valido" });
    }

    if (!from || !to) {
      return res.status(400).json({ ok: false, error: "Faltan fechas from y to" });
    }

    const sql = `
      SELECT
        r.id,
        r.created_at,
        r.timestamp_ms,
        r.device_id,
        m.device_name AS device_name,
        r.pm_slave,
        m.pm_name AS pm_name,

        r.voltage_a,
        r.voltage_b,
        r.voltage_c,

        r.current_a,
        r.current_b,
        r.current_c,
        r.current_n,

        r.p_a,
        r.p_b,
        r.p_c,
        r.p_tot,

        r.q_a,
        r.q_b,
        r.q_c,
        r.q_tot,

        r.s_a,
        r.s_b,
        r.s_c,
        r.s_tot,

        r.pf_a,
        r.pf_b,
        r.pf_c,
        r.pf_tot,

        r.frecuencia,
        r.active_energy,

        r.thd_va,
        r.thd_vb,
        r.thd_vc,

        r.thd_ia,
        r.thd_ib,
        r.thd_ic,
        r.thd_in,

        r.desbalance_v,
        r.desbalance_i

      FROM power_readings r
      LEFT JOIN power_meters m
        ON r.device_id = m.device_id
       AND r.pm_slave = m.pm_slave
      WHERE r.device_id = $1
        AND r.pm_slave = $2
        AND r.created_at >= (($3::date)::timestamp AT TIME ZONE 'America/Guayaquil')
        AND r.created_at < (((($4::date) + INTERVAL '1 day')::timestamp) AT TIME ZONE 'America/Guayaquil')
      ORDER BY r.created_at ASC, r.id ASC
    `;

    const values = [device_id, pmSlave, from, to];
    const result = await pool.query(sql, values);
    const rows = result.rows;

    const fileName = `${device_id}_PM${pmSlave}_${from}_to_${to}.csv`;

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
    res.write("﻿");

    const headers = [
      "created_at",
      "timestamp_ms",
      "device_id",
      "device_name",
      "pm_slave",
      "pm_name",
      "voltage_a",
      "voltage_b",
      "voltage_c",
      "current_a",
      "current_b",
      "current_c",
      "current_n",
      "p_a",
      "p_b",
      "p_c",
      "p_tot",
      "q_a",
      "q_b",
      "q_c",
      "q_tot",
      "s_a",
      "s_b",
      "s_c",
      "s_tot",
      "pf_a",
      "pf_b",
      "pf_c",
      "pf_tot",
      "frecuencia",
      "active_energy",
      "thd_va",
      "thd_vb",
      "thd_vc",
      "thd_ia",
      "thd_ib",
      "thd_ic",
      "thd_in",
      "desbalance_v",
      "desbalance_i"
    ];

    res.write(headers.join(",") + "\n");

    for (const row of rows) {
      const rowValues = [
        row.created_at
          ? new Date(row.created_at).toLocaleString("es-EC", {
              timeZone: "America/Guayaquil"
            })
          : "",
        row.timestamp_ms ?? "",
        row.device_id ?? "",
        row.device_name ?? "",
        row.pm_slave ?? "",
        row.pm_name ?? "",

        row.voltage_a ?? "",
        row.voltage_b ?? "",
        row.voltage_c ?? "",

        row.current_a ?? "",
        row.current_b ?? "",
        row.current_c ?? "",
        row.current_n ?? "",

        row.p_a ?? "",
        row.p_b ?? "",
        row.p_c ?? "",
        row.p_tot ?? "",

        row.q_a ?? "",
        row.q_b ?? "",
        row.q_c ?? "",
        row.q_tot ?? "",

        row.s_a ?? "",
        row.s_b ?? "",
        row.s_c ?? "",
        row.s_tot ?? "",

        row.pf_a ?? "",
        row.pf_b ?? "",
        row.pf_c ?? "",
        row.pf_tot ?? "",

        row.frecuencia ?? "",
        row.active_energy ?? "",

        row.thd_va ?? "",
        row.thd_vb ?? "",
        row.thd_vc ?? "",

        row.thd_ia ?? "",
        row.thd_ib ?? "",
        row.thd_ic ?? "",
        row.thd_in ?? "",

        row.desbalance_v ?? "",
        row.desbalance_i ?? ""
      ].map(value => {
        const text = String(value);
        return `"${text.replace(/"/g, '""')}"`;
      });

      res.write(rowValues.join(",") + "\n");
    }

    res.end();
  } catch (error) {
    console.error("Error exportando CSV:", error);
    res.status(500).json({
      ok: false,
      error: "Error exportando CSV",
      detail: error.message
    });
  }
});

// ============================================================
// BLOQUE 12B: API - EXPORTAR HISTORICO PEAK EN CSV POR RANGO
// ============================================================
app.get("/api/peak/history/export", async (req, res) => {
  try {
    const { device_id, from, to } = req.query;

    if (!device_id) {
      return res.status(400).json({ ok: false, error: "Falta device_id" });
    }

    if (!from || !to) {
      return res.status(400).json({ ok: false, error: "Faltan fechas from y to" });
    }

    const sql = `
      SELECT
        r.id,
        r.created_at,
        r.device_id,
        d.device_name AS device_name,
        r.counter_index,
        c.slave_id,
        c.counter_name,
        CASE
          WHEN LOWER(c.counter_type) = 'fmpro' THEN 'FMPROTECTION'
          WHEN LOWER(c.counter_type) = 'tstlp' THEN 'TSTLP'
          ELSE c.counter_type
        END AS counter_type,
        r.counter_value,
        r.online

      FROM peak_readings r
      LEFT JOIN peak_counters c
        ON r.device_id = c.device_id
       AND r.counter_index = c.counter_index
      LEFT JOIN peak_devices d
        ON r.device_id = d.device_id
      WHERE r.device_id = $1
        AND r.created_at >= (($2::date)::timestamp AT TIME ZONE 'America/Guayaquil')
        AND r.created_at < (((($3::date) + INTERVAL '1 day')::timestamp) AT TIME ZONE 'America/Guayaquil')
      ORDER BY r.counter_index ASC, r.created_at ASC, r.id ASC
    `;

    const values = [device_id, from, to];
    const result = await pool.query(sql, values);
    const rows = result.rows;

    const fileName = `${device_id}_PEAK_${from}_to_${to}.csv`;

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
    res.write("﻿");

    const headers = [
      "created_at",
      "device_id",
      "device_name",
      "counter_index",
      "slave_id",
      "counter_name",
      "counter_type",
      "counter_value",
      "online"
    ];

    res.write(headers.join(",") + "\n");

    for (const row of rows) {
      const rowValues = [
        row.created_at
          ? new Date(row.created_at).toLocaleString("es-EC", {
              timeZone: "America/Guayaquil"
            })
          : "",
        row.device_id ?? "",
        row.device_name ?? "",
        row.counter_index ?? "",
        row.slave_id ?? "",
        row.counter_name ?? "",
        row.counter_type ?? "",
        row.counter_value ?? "",
        row.online ?? ""
      ].map(value => {
        const text = String(value);
        return `"${text.replace(/"/g, '""')}"`;
      });

      res.write(rowValues.join(",") + "\n");
    }

    res.end();
  } catch (error) {
    console.error("Error exportando CSV PEAK:", error);
    res.status(500).json({
      ok: false,
      error: "Error exportando CSV PEAK",
      detail: error.message
    });
  }
});

// ============================================================
// BLOQUE 12C: AUTH DASHBOARD (TOKEN)
// ============================================================
app.get("/api/dashboard-auth", async (req, res) => {
  try {
    const device_id = String(req.query.device_id || "").trim();
    const token = String(req.query.token || "").trim();

    if (!device_id || !token) {
      return res.status(400).json({
        ok: false,
        error: "missing_params",
        message: "Faltan device_id o token"
      });
    }

    const sql = `
      SELECT device_id
      FROM power_meters
      WHERE device_id = $1
        AND token = $2
      LIMIT 1
    `;

    const result = await pool.query(sql, [device_id, token]);

    if (result.rowCount === 0) {
      return res.status(401).json({
        ok: false,
        error: "token_incorrecto",
        message: "Token incorrecto para este dispositivo"
      });
    }

    return res.json({
      ok: true,
      message: "Acceso concedido",
      device_id
    });

  } catch (err) {
    console.error("Error en /api/dashboard-auth:", err);
    return res.status(500).json({
      ok: false,
      error: "server_error",
      message: "Error interno validando acceso"
    });
  }
});

// ============================================================
// BLOQUE 12D: AUTH DASHBOARD PEAK
// ============================================================
app.get("/api/peak/dashboard-auth", async (req, res) => {
  try {
    const device_id = String(req.query.device_id || "").trim();
    const token     = String(req.query.token || "").trim();

    if (!device_id || !token) {
      return res.status(400).json({
        ok: false,
        error: "missing_params",
        message: "Faltan device_id o token"
      });
    }

    const sql = `
      SELECT device_id
      FROM peak_devices
      WHERE device_id = $1
        AND token = $2
      LIMIT 1
    `;

    const result = await pool.query(sql, [device_id, token]);

    if (result.rowCount === 0) {
      return res.status(401).json({
        ok: false,
        error: "token_incorrecto",
        message: "Token incorrecto para este gateway"
      });
    }

    return res.json({
      ok: true,
      message: "Acceso concedido",
      device_id
    });

  } catch (err) {
    console.error("Error en /api/peak/dashboard-auth:", err);
    return res.status(500).json({
      ok: false,
      error: "server_error",
      message: "Error interno validando acceso"
    });
  }
});

// ============================================================
// BLOQUE 12E: API PEAK - GUARDAR LECTURA
// ============================================================
app.post("/api/peak/save-reading", async (req, res) => {
  try {
    const data = req.body;
    const counters = Array.isArray(data.counters) ? data.counters : [];

    if (!data.device_id) {
      return res.status(400).json({
        ok: false,
        error: "Falta device_id"
      });
    }

    if (counters.length === 0) {
      return res.status(400).json({
        ok: false,
        error: "Faltan counters"
      });
    }

    await upsertPeakDevice(data);

    const inserted = [];

    for (const counter of counters) {
      if (counter.index === undefined || counter.index === null) {
        continue;
      }

      await upsertPeakCounter(data.device_id, counter);
      const result = await insertPeakReading(data, counter);

      inserted.push({
        counter_index: counter.index,
        slave_id: counter.slave_id ?? null,
        counter_name: counter.name ?? null,
        id: result.rows[0].id,
        created_at: result.rows[0].created_at
      });
    }

    return res.json({
      ok: true,
      device_id: data.device_id,
      device_name: data.device_name ?? null,
      total_counters_received: counters.length,
      total_counters_saved: inserted.length,
      inserted
    });

  } catch (error) {
    console.error("Error guardando lectura PEAK:", error);
    return res.status(500).json({
      ok: false,
      error: "Error guardando lectura PEAK",
      detail: error.message
    });
  }
});

// ============================================================
// BLOQUE 12F: API PEAK - LISTAR CONTADORES DE UN DEVICE
// ============================================================
app.get("/api/peak/counters/:device_id", async (req, res) => {
  try {
    const { device_id } = req.params;

    const result = await pool.query(
      `
      SELECT
        device_id,
        counter_index,
        slave_id,
        counter_name,
        counter_type,
        created_at,
        updated_at
      FROM peak_counters
      WHERE device_id = $1
      ORDER BY counter_index ASC
      `,
      [device_id]
    );

    return res.json({
      ok: true,
      device_id,
      total: result.rows.length,
      data: result.rows
    });
  } catch (error) {
    console.error("Error consultando contadores PEAK:", error);
    return res.status(500).json({
      ok: false,
      error: "Error consultando contadores PEAK",
      detail: error.message
    });
  }
});

// ============================================================
// BLOQUE 12G: API PEAK - ULTIMO VALOR DE UN CONTADOR
// ============================================================
app.get("/api/peak/device/:device_id", async (req, res) => {
  try {
    const { device_id } = req.params;
    const counterIndex = Number(req.query.counter_index);

    if (!Number.isInteger(counterIndex)) {
      return res.status(400).json({
        ok: false,
        error: "Falta counter_index valido en query"
      });
    }

    const sql = `
      SELECT
        r.id,
        r.device_id,
        r.device_name,
        r.token,
        r.model,
        r.fw,
        r.conn_mode,
        r.ip,
        r.rssi,
        r.uptime_ms,
        r.device_timestamp_ms,

        r.counter_index,
        r.slave_id,
        r.counter_name,
        r.counter_type,
        r.counter_value,
        r.online,
        r.fail_count,

        r.raw_payload,
        r.created_at,
        r.created_at AS visible_at

      FROM peak_readings r
      WHERE r.device_id = $1
        AND r.counter_index = $2
      ORDER BY r.created_at DESC, r.id DESC
      LIMIT 1
    `;

    const result = await pool.query(sql, [device_id, counterIndex]);

    if (result.rows.length === 0) {
      return res.json({
        ok: false,
        error: "No se encontraron datos del contador",
        device_id,
        counter_index: counterIndex
      });
    }

    return res.json({
      ok: true,
      device_id,
      counter_index: counterIndex,
      data: result.rows[0]
    });
  } catch (error) {
    console.error("Error consultando ultimo valor PEAK:", error);
    return res.status(500).json({
      ok: false,
      error: "Error consultando ultimo valor PEAK",
      detail: error.message
    });
  }
});

// ============================================================
// BLOQUE 12H: API PEAK - HISTORICO POR CONTADOR
// ============================================================
app.get("/api/peak/history", async (req, res) => {
  try {
    const { device_id, from, to } = req.query;
    const counterIndex = Number(req.query.counter_index);

    if (!device_id) {
      return res.status(400).json({ ok: false, error: "Falta device_id" });
    }

    if (!Number.isInteger(counterIndex)) {
      return res.status(400).json({ ok: false, error: "Falta counter_index valido" });
    }

    let sql = `
      SELECT
        r.id,
        r.device_id,
        r.device_name,
        r.token,
        r.model,
        r.fw,
        r.conn_mode,
        r.ip,
        r.rssi,
        r.uptime_ms,
        r.device_timestamp_ms,

        r.counter_index,
        r.slave_id,
        r.counter_name,
        r.counter_type,
        r.counter_value,
        r.online,
        r.fail_count,

        r.raw_payload,
        r.created_at,
        r.created_at AS visible_at

      FROM peak_readings r
      WHERE r.device_id = $1
        AND r.counter_index = $2
    `;

    const values = [device_id, counterIndex];

    if (from && to) {
      sql += `
        AND r.created_at >= (($3::date)::timestamp AT TIME ZONE 'America/Guayaquil')
        AND r.created_at < (((($4::date) + INTERVAL '1 day')::timestamp) AT TIME ZONE 'America/Guayaquil')
      `;
      values.push(from, to);
    }

    sql += ` ORDER BY r.created_at ASC, r.id ASC`;

    const result = await pool.query(sql, values);

    return res.json({
      ok: true,
      device_id,
      counter_index: counterIndex,
      total: result.rows.length,
      data: result.rows
    });
  } catch (error) {
    console.error("Error consultando historico PEAK:", error);
    return res.status(500).json({
      ok: false,
      error: "Error consultando historico PEAK",
      detail: error.message
    });
  }
});

// ============================================================
// BLOQUE 12I: API v1 - AUTH DE DISPOSITIVOS (gateways.api_key_hash)
// ============================================================
app.post("/api/v1/ingest/auth", async (req, res) => {
  try {
    const data = req.body || {};
    const deviceId = String(data.device_id || "").trim();
    const token = String(data.token || "").trim();

    if (!deviceId || !token) {
      return res.status(400).json({
        ok: false,
        error: "missing_params",
        message: "Faltan device_id o token"
      });
    }

    const gatewayResult = await pool.query(
      `SELECT id, api_key_hash FROM gateways WHERE device_id = $1 LIMIT 1`,
      [deviceId]
    );

    if (gatewayResult.rowCount === 0) {
      return res.status(404).json({
        ok: false,
        error: "device_no_registrado",
        message: "Device ID no registrado"
      });
    }

    const gateway = gatewayResult.rows[0];

    if (!gateway.api_key_hash) {
      return res.status(401).json({
        ok: false,
        error: "device_sin_credenciales",
        message: "Gateway sin credenciales configuradas"
      });
    }

    const tokenHash = crypto.createHash("sha256").update(token).digest("hex");
    const expectedHash = Buffer.from(gateway.api_key_hash, "utf8");
    const actualHash = Buffer.from(tokenHash, "utf8");

    const tokenValido =
      expectedHash.length === actualHash.length &&
      crypto.timingSafeEqual(expectedHash, actualHash);

    if (!tokenValido) {
      return res.status(401).json({
        ok: false,
        error: "token_incorrecto",
        message: "Token incorrecto"
      });
    }

    if (data.pm_slave !== undefined && data.pm_slave !== null) {
      const pmSlave = Number(data.pm_slave);

      if (!Number.isInteger(pmSlave)) {
        return res.status(400).json({
          ok: false,
          error: "pm_slave_invalido",
          message: "pm_slave debe ser entero"
        });
      }

      const meterResult = await pool.query(
        `SELECT 1 FROM meters WHERE gateway_id = $1 AND pm_slave = $2 LIMIT 1`,
        [gateway.id, pmSlave]
      );

      if (meterResult.rowCount === 0) {
        return res.status(403).json({
          ok: false,
          error: "pm_slave_no_autorizado",
          message: "PM slave no autorizado"
        });
      }
    }

    return res.json({
      ok: true,
      error: null,
      message: "Validacion correcta",
      device_id: deviceId,
      gateway_id: gateway.id
    });

  } catch (error) {
    console.error("Error en /api/v1/ingest/auth:", error);
    return res.status(500).json({
      ok: false,
      error: "server_error",
      message: "Error interno validando credenciales"
    });
  }
});

// ============================================================
// BLOQUE 12.1: CONFIGURACION DE NOTIFICACIONES (V1.2 reducida)
// Protegido con header X-Internal-Key, ya que expone tokens de
// bots de Telegram. Solo Node-RED debe llamar a este endpoint.
// ============================================================
app.get("/api/v1/ingest/notification-config", async (req, res) => {
  try {
    if (!verificarInternalKey(req)) {
      return res.status(403).json({ ok: false, error: "forbidden" });
    }

    const result = await pool.query(`
      SELECT g.device_id, nc.telegram_bot_token, nc.telegram_chat_id
      FROM gateways g
      JOIN sites s ON s.id = g.site_id
      JOIN notification_channels nc ON nc.organization_id = s.organization_id
    `);

    return res.json(result.rows);

  } catch (error) {
    console.error("Error en /api/v1/ingest/notification-config:", error);
    return res.status(500).json({ ok: false, error: "server_error" });
  }
});

// ============================================================
// BLOQUE 12.2: POST /api/v1/ingest/connectivity-event (V1.5)
//
// Recibe eventos de conectividad desde Node-RED (pm_offline,
// pm_recovered, device_timeout, device_timeout_repeat,
// device_recovered) y envía la notificación Telegram centralizada.
// Centraliza toda la lógica de notificación en Servicio B, quitando
// la dependencia de `notification_config` en el flow context de Node-RED.
// ============================================================
app.post("/api/v1/ingest/connectivity-event", async (req, res) => {
  const internalKey = req.headers["x-internal-key"];
  if (!internalKey || internalKey !== process.env.INTERNAL_API_KEY) {
    return res.status(401).json({ error: "no_autorizado" });
  }

  const { device_id, tipo, pm_slave, pm_name, device_name, timestamp } = req.body || {};
  if (!device_id || !tipo) {
    return res.status(400).json({ error: "faltan_campos", required: ["device_id", "tipo"] });
  }

  try {
    const { rows } = await pool.query(
      `SELECT g.id AS gateway_id, s.organization_id
       FROM gateways g
       JOIN sites s ON s.id = g.site_id
       WHERE g.device_id = $1
       LIMIT 1`,
      [device_id]
    );
    if (!rows.length) {
      return res.status(404).json({ error: "device_no_registrado" });
    }
    const { gateway_id: gatewayId, organization_id: organizationId } = rows[0];

    const fecha = new Date(timestamp || Date.now());
    const hora = fecha.toLocaleString("es-EC", {
      timeZone: "America/Guayaquil",
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    });

    let mensaje = "";
    switch (tipo) {
      case "pm_offline":
        mensaje = `⚠️ PM OFFLINE\nDevice: ${device_id}\nPM: ${pm_slave}\nNombre: ${pm_name || "N/A"}\nHora: ${hora}`;
        break;
      case "pm_recovered":
        mensaje = `✅ PM RECUPERADO\nDevice: ${device_id}\nPM: ${pm_slave}\nNombre: ${pm_name || "N/A"}\nHora: ${hora}`;
        break;
      case "device_timeout":
        mensaje = `🚨 DEVICE CAÍDO\nDevice: ${device_id}\nHora: ${hora}`;
        break;
      case "device_timeout_repeat":
        mensaje = `🔁 DEVICE SIGUE CAÍDO\nDevice: ${device_id}\nHora: ${hora}`;
        break;
      case "device_recovered":
        mensaje = `✅ DEVICE RECUPERADO\nDevice: ${device_id}\nHora: ${hora}`;
        break;
      default:
        mensaje = `ℹ️ Evento ${tipo}\nDevice: ${device_id}\nHora: ${hora}`;
    }

    await dispatch(organizationId, { gatewayId }, mensaje);
    console.log(`[EVENT] ${tipo} — device=${device_id} pm=${pm_slave ?? "-"}`);
    res.json({ ok: true });
  } catch (e) {
    console.error("[EVENT] /connectivity-event error:", e.message);
    res.status(500).json({ error: "error_interno" });
  }
});

// ============================================================
// BLOQUE 13: EVALUADOR DE ALARMAS (V1.4)
//
// Corre cada ALARM_EVAL_INTERVAL_MS (default 2 min) vía setInterval.
// Lee alarm_rules habilitadas, compara el valor actual en power_latest,
// crea alarm_events al disparar y los resuelve cuando la condición
// deja de cumplirse. Notifica vía Telegram al disparar.
//
// La interpolación de `rule.variable` en SQL es segura: el CHECK
// constraint de alarm_rules.variable garantiza que solo son nombres
// válidos de columnas de power_latest. ALARM_VARIABLES_PERMITIDAS
// agrega una segunda capa de validación en JS.
// ============================================================

const ALARM_EVAL_INTERVAL_MS  = parseInt(process.env.ALARM_EVAL_INTERVAL_MS  || "120000",  10);
const ALARM_RENOTIF_INTERVAL_MS = parseInt(process.env.ALARM_RENOTIF_INTERVAL_MS || "900000", 10); // 15 min default

const ALARM_VARIABLES_PERMITIDAS = new Set([
  "voltage_a", "voltage_b", "voltage_c",
  "current_a", "current_b", "current_c", "current_n",
  "p_a", "p_b", "p_c", "p_tot",
  "q_a", "q_b", "q_c", "q_tot",
  "s_a", "s_b", "s_c", "s_tot",
  "pf_a", "pf_b", "pf_c", "pf_tot",
  "frecuencia", "active_energy",
  "thd_va", "thd_vb", "thd_vc",
  "thd_ia", "thd_ib", "thd_ic", "thd_in",
  "desbalance_v", "desbalance_i",
]);

// ---- Dispatcher V1.6: multi-canal (Telegram + Webhook) ----

async function enviarTelegram(token, chatId, mensaje) {
  const body = JSON.stringify({ chat_id: chatId, text: mensaje });
  await new Promise((resolve) => {
    const req = https.request(
      {
        hostname: "api.telegram.org",
        path: `/bot${token}/sendMessage`,
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(body),
        },
      },
      (res) => { res.resume(); resolve(); }
    );
    req.on("error", (e) => { console.error("[NOTIF] Telegram error:", e.message); resolve(); });
    req.write(body);
    req.end();
  });
}

async function enviarWebhook(webhookUrl, mensaje, organizationId) {
  const parsed = new URL(webhookUrl);
  const body = JSON.stringify({ text: mensaje, organization_id: organizationId });
  await new Promise((resolve) => {
    const mod = parsed.protocol === "https:" ? https : require("http");
    const req = mod.request(
      {
        hostname: parsed.hostname,
        path: parsed.pathname + (parsed.search || ""),
        port: parsed.port || (parsed.protocol === "https:" ? 443 : 80),
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(body),
        },
      },
      (res) => { res.resume(); resolve(); }
    );
    req.on("error", (e) => { console.error("[NOTIF] Webhook error:", e.message); resolve(); });
    req.write(body);
    req.end();
  });
}

// dispatch(organizationId, { alarmRuleId, gatewayId }, mensaje)
//
// Orden de resolución de canales:
//   1. Si alarmRuleId → notification_targets WHERE alarm_rule_id = ?
//   2. Si gatewayId   → notification_targets WHERE gateway_id = ?
//   3. Fallback       → todos los canales habilitados de la organización
//
// El fallback garantiza que las reglas/gateways sin targets configurados
// sigan notificando (comportamiento pre-V1.6).
async function dispatch(organizationId, { alarmRuleId = null, gatewayId = null } = {}, mensaje) {
  try {
    let canales = [];

    if (alarmRuleId) {
      const { rows } = await pool.query(
        `SELECT nc.channel_type, nc.telegram_bot_token, nc.telegram_chat_id, nc.webhook_url
         FROM notification_channels nc
         JOIN notification_targets nt ON nt.channel_id = nc.id
         WHERE nt.alarm_rule_id = $1 AND nc.enabled = true`,
        [alarmRuleId]
      );
      canales = rows;
    } else if (gatewayId) {
      const { rows } = await pool.query(
        `SELECT nc.channel_type, nc.telegram_bot_token, nc.telegram_chat_id, nc.webhook_url
         FROM notification_channels nc
         JOIN notification_targets nt ON nt.channel_id = nc.id
         WHERE nt.gateway_id = $1 AND nc.enabled = true`,
        [gatewayId]
      );
      canales = rows;
    }

    if (!canales.length) {
      const { rows } = await pool.query(
        `SELECT channel_type, telegram_bot_token, telegram_chat_id, webhook_url
         FROM notification_channels
         WHERE organization_id = $1 AND enabled = true`,
        [organizationId]
      );
      canales = rows;
    }

    for (const ch of canales) {
      if (ch.channel_type === "telegram" && ch.telegram_bot_token && ch.telegram_chat_id) {
        await enviarTelegram(ch.telegram_bot_token, ch.telegram_chat_id, mensaje);
      } else if (ch.channel_type === "webhook" && ch.webhook_url) {
        await enviarWebhook(ch.webhook_url, mensaje, organizationId);
      }
    }
  } catch (e) {
    console.error("[NOTIF] dispatch error:", e.message);
  }
}

// Evalúa una sola regla con el valor provisto (ya leído de power_latest).
// Si valor es null, solo resuelve eventos abiertos (lectura sin dato eléctrico).
async function evaluarRegla(regla, valor) {
  const umbral = parseFloat(regla.threshold);
  if (isNaN(umbral)) return;

  let condicion = false;
  if (valor !== null && !isNaN(valor)) {
    if (regla.operator === ">")  condicion = valor > umbral;
    if (regla.operator === "<")  condicion = valor < umbral;
    if (regla.operator === ">=") condicion = valor >= umbral;
    if (regla.operator === "<=") condicion = valor <= umbral;
  }

  const { rows: abiertos } = await pool.query(
    "SELECT id FROM alarm_events WHERE alarm_rule_id = $1 AND resolved_at IS NULL LIMIT 1",
    [regla.id]
  );
  const hayAbierto = abiertos.length > 0;

  if (condicion && !hayAbierto) {
    await pool.query(
      `INSERT INTO alarm_events (alarm_rule_id, organization_id, meter_id, variable, value_at_trigger)
       VALUES ($1, $2, $3, $4, $5)`,
      [regla.id, regla.organization_id, regla.meter_id, regla.variable, valor]
    );
    console.log(`[ALARM] Disparo — ${regla.name}: ${regla.variable} ${regla.operator} ${umbral} (valor: ${valor})`);

    const msg =
      `⚠️ ALARMA: ${regla.name}\n` +
      `Medidor: ${regla.device_id} / PM ${regla.pm_slave}\n` +
      `${regla.variable} ${regla.operator} ${umbral} — valor actual: ${valor}`;
    await dispatch(regla.organization_id, { alarmRuleId: regla.id }, msg);

    await pool.query(
      "UPDATE alarm_events SET notified_at = now() WHERE alarm_rule_id = $1 AND resolved_at IS NULL",
      [regla.id]
    );

  } else if (condicion && hayAbierto) {
    // Condición sigue activa — re-notificar si pasó más del intervalo configurado en el canal.
    const { rows: ev } = await pool.query(
      "SELECT notified_at FROM alarm_events WHERE alarm_rule_id = $1 AND resolved_at IS NULL LIMIT 1",
      [regla.id]
    );
    const lastNotif = ev[0]?.notified_at;
    const elapsed = lastNotif ? Date.now() - new Date(lastNotif).getTime() : Infinity;

    // Leer intervalo desde la DB (MIN entre canales activos de la org); fallback al env var.
    const { rows: chRows } = await pool.query(
      "SELECT MIN(renotif_interval_minutes) AS minutes FROM notification_channels WHERE organization_id = $1 AND enabled = true",
      [regla.organization_id]
    );
    const renotifMs = chRows[0]?.minutes != null
      ? chRows[0].minutes * 60 * 1000
      : ALARM_RENOTIF_INTERVAL_MS;

    if (elapsed >= renotifMs) {
      const msg =
        `🔔 RECORDATORIO — Alarma activa: ${regla.name}\n` +
        `Medidor: ${regla.device_id} / PM ${regla.pm_slave}\n` +
        `${regla.variable} ${regla.operator} ${umbral} — valor actual: ${valor}`;
      await dispatch(regla.organization_id, { alarmRuleId: regla.id }, msg);
      await pool.query(
        "UPDATE alarm_events SET notified_at = now() WHERE alarm_rule_id = $1 AND resolved_at IS NULL",
        [regla.id]
      );
      console.log(`[ALARM] Recordatorio — ${regla.name}: ${regla.variable} sigue ${regla.operator} ${umbral} (valor: ${valor})`);
    }

  } else if (!condicion && hayAbierto) {
    await pool.query(
      "UPDATE alarm_events SET resolved_at = now() WHERE alarm_rule_id = $1 AND resolved_at IS NULL",
      [regla.id]
    );
    console.log(`[ALARM] Resuelto — ${regla.name}: ${regla.variable} volvio a ${valor} (umbral: ${regla.operator} ${umbral})`);
  }
}

// Evalúa solo las reglas del device_id/pm_slave que acaba de reportar.
// Se llama desde /api/save-reading con el payload ya disponible en memoria,
// sin releer power_latest (el valor recién escrito se pasa directamente).
async function evaluarAlarmasParaDevice(deviceId, pmSlave, payload) {
  try {
    const { rows: reglas } = await pool.query(`
      SELECT
        ar.id, ar.organization_id, ar.meter_id, ar.name,
        ar.variable, ar.operator, ar.threshold,
        m.pm_slave, g.device_id
      FROM alarm_rules ar
      JOIN meters m ON m.id = ar.meter_id
      JOIN gateways g ON g.id = m.gateway_id
      WHERE ar.enabled = true
        AND g.device_id = $1
        AND m.pm_slave = $2
    `, [deviceId, pmSlave]);

    for (const regla of reglas) {
      if (!ALARM_VARIABLES_PERMITIDAS.has(regla.variable)) continue;
      const raw = payload[regla.variable];
      const valor = (raw !== undefined && raw !== null) ? parseFloat(raw) : null;
      await evaluarRegla(regla, valor);
    }
  } catch (e) {
    console.error("[ALARM] evaluarAlarmasParaDevice error:", e.message);
  }
}

// Sweeper global: evalúa todas las reglas habilitadas leyendo power_latest.
// Corre cada ALARM_EVAL_INTERVAL_MS como red de seguridad (cold starts,
// reglas creadas mientras no llegaban lecturas del device, etc.).
async function evaluarAlarmas() {
  try {
    const { rows: reglas } = await pool.query(`
      SELECT
        ar.id, ar.organization_id, ar.meter_id, ar.name,
        ar.variable, ar.operator, ar.threshold,
        m.pm_slave, g.device_id
      FROM alarm_rules ar
      JOIN meters m ON m.id = ar.meter_id
      JOIN gateways g ON g.id = m.gateway_id
      WHERE ar.enabled = true
    `);

    for (const regla of reglas) {
      if (!ALARM_VARIABLES_PERMITIDAS.has(regla.variable)) continue;

      const { rows: latest } = await pool.query(
        `SELECT ${regla.variable} AS valor FROM power_latest WHERE device_id = $1 AND pm_slave = $2 LIMIT 1`,
        [regla.device_id, regla.pm_slave]
      );

      const raw = latest.length ? latest[0].valor : null;
      const valor = (raw !== null && raw !== undefined) ? parseFloat(raw) : null;
      await evaluarRegla(regla, valor);
    }
  } catch (e) {
    console.error("[ALARM] evaluarAlarmas error:", e.message);
  }
}

// ============================================================
// BLOQUE 14: INICIO DEL SERVIDOR
// ============================================================
async function iniciar() {
  await probarPostgres();
  await crearTablasSiNoExisten();
  await crearTablasPeakSiNoExisten();
}

const PORT = process.env.PORT || 3000;

app.listen(PORT, async () => {
  console.log("API corriendo en puerto " + PORT);
  await iniciar();
  evaluarAlarmas();
  setInterval(evaluarAlarmas, ALARM_EVAL_INTERVAL_MS);
  console.log(`Evaluador de alarmas activo (cada ${ALARM_EVAL_INTERVAL_MS / 1000}s)`);
});
