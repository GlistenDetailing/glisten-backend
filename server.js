const express = require("express");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");
const cors = require("cors");

const app = express();
const PORT = process.env.PORT || 4000;


// Middleware
app.use(cors());
app.use(express.json());

// Database
const dbPath = process.env.DATABASE_PATH || path.join(__dirname, "glisten.db");
const db = new sqlite3.Database(dbPath);

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS bookings (
      id TEXT PRIMARY KEY,
      createdAt TEXT NOT NULL,
      status TEXT NOT NULL,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      phone TEXT NOT NULL,
      postcode TEXT NOT NULL,
      preferredDate TEXT NOT NULL,
      carMake TEXT,
      carModel TEXT,
      notes TEXT,
      servicesJson TEXT NOT NULL,
      amendNotes TEXT
    )
  `);
});

// Generate booking ID
function generateBookingId(cb) {
  db.get("SELECT COUNT(*) as count FROM bookings", (err, row) => {
    if (err) return cb(err);
    const n = (row.count || 0) + 1;
    const id = "GLSTN-" + String(n).padStart(6, "0");
    cb(null, id);
  });
}

// Create booking
app.post("/api/bookings", (req, res) => {
  const {
    name, email, phone, postcode, preferredDate,
    carMake, carModel, notes, services
  } = req.body;

  if (!name || !email || !phone || !postcode || !preferredDate || !services) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  generateBookingId((err, id) => {
    if (err) return res.status(500).json({ error: "Failed to generate ID" });

    const createdAt = new Date().toISOString();
    const status = "pending";

    db.run(
      `
      INSERT INTO bookings (
        id, createdAt, status, name, email, phone, postcode,
        preferredDate, carMake, carModel, notes, servicesJson
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        id,
        createdAt,
        status,
        name,
        email,
        phone,
        postcode,
        preferredDate,
        carMake || null,
        carModel || null,
        notes || null,
        JSON.stringify(services)
      ],
      (err2) => {
        if (err2) return res.status(500).json({ error: "Save failed" });
        res.json({ bookingId: id, status });
      }
    );
  });
});

// Client amend / cancel request
app.post("/api/bookings/:id/amend", (req, res) => {
  const { id } = req.params;
  const { email, action, details } = req.body;

  if (!email || !action) {
    return res.status(400).json({ error: "Missing fields" });
  }

  const newStatus =
    action === "cancel" ? "cancel_requested" : "change_requested";

  db.get(
    "SELECT * FROM bookings WHERE id = ? AND email = ?",
    [id, email],
    (err, booking) => {
      if (err) return res.status(500).json({ error: "DB error" });
      if (!booking) {
        return res.status(404).json({ error: "Booking not found" });
      }

      db.run(
        "UPDATE bookings SET status = ?, amendNotes = ? WHERE id = ?",
        [newStatus, details || null, id],
        (err2) => {
          if (err2) return res.status(500).json({ error: "Update failed" });
          res.json({ ok: true });
        }
      );
    }
  );
});

// Admin – List bookings
app.get("/api/bookings", (req, res) => {
  const { status } = req.query;
  let sql = "SELECT * FROM bookings";
  const params = [];

  if (status) {
    sql += " WHERE status = ?";
    params.push(status);
  }

  sql += " ORDER BY createdAt DESC";

  db.all(sql, params, (err, rows) => {
    if (err) return res.status(500).json({ error: "DB error" });

    const out = rows.map((row) => ({
      ...row,
      services: JSON.parse(row.servicesJson)
    }));
    res.json(out);
  });
});

// Admin – Confirm booking
app.post("/api/bookings/:id/confirm", (req, res) => {
  const { id } = req.params;

  db.run(
    "UPDATE bookings SET status = ? WHERE id = ?",
    ["confirmed", id],
    (err) => {
      if (err) return res.status(500).json({ error: "Update failed" });
      res.json({ ok: true });
    }
  );
});

// Admin – Reject booking
app.post("/api/bookings/:id/reject", (req, res) => {
  const { id } = req.params;
  const { reason } = req.body;

  db.run(
    "UPDATE bookings SET status = ?, amendNotes = ? WHERE id = ?",
    ["rejected", reason || null, id],
    (err) => {
      if (err) return res.status(500).json({ error: "Update failed" });
      res.json({ ok: true });
    }
  );
});

// Simple health-check endpoint
app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});


app.listen(PORT, () => {
  console.log("Glisten backend running on http://localhost:" + PORT);
});
