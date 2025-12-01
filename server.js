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

// --- Root endpoint for Render ---
app.get("/", (req, res) => {
  res.send("Glisten backend is running");
});

// Create bookings table
db.run(
  `CREATE TABLE IF NOT EXISTS bookings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    email TEXT,
    phone TEXT,
    postcode TEXT,
    car_make TEXT,
    car_model TEXT,
    services TEXT,
    preferred_date TEXT,
    preferred_time TEXT,
    status TEXT DEFAULT 'pending'
  )`
);

// Create amend table
db.run(
  `CREATE TABLE IF NOT EXISTS amendments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    booking_id INTEGER,
    new_date TEXT,
    new_time TEXT,
    message TEXT
  )`
);

// POST booking request
app.post("/api/bookings", (req, res) => {
  const {
    name,
    email,
    phone,
    postcode,
    car_make,
    car_model,
    services,
    preferred_date,
    preferred_time,
  } = req.body;

  db.run(
    `INSERT INTO bookings 
     (name, email, phone, postcode, car_make, car_model, services, preferred_date, preferred_time)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      name,
      email,
      phone,
      postcode,
      car_make,
      car_model,
      JSON.stringify(services),
      preferred_date,
      preferred_time,
    ],
    function (err) {
      if (err) {
        return res.status(500).json({ error: "Failed to create booking" });
      }

      res.json({ booking_id: this.lastID });
    }
  );
});

// GET booking by ID
app.get("/api/bookings/:id", (req, res) => {
  const bookingId = req.params.id;

  db.get(`SELECT * FROM bookings WHERE id = ?`, [bookingId], (err, row) => {
    if (err || !row) return res.status(404).json({ error: "Not found" });
    res.json(row);
  });
});

// POST amend booking
app.post("/api/amend", (req, res) => {
  const { booking_id, new_date, new_time, message } = req.body;

  db.run(
    `INSERT INTO amendments (booking_id, new_date, new_time, message)
     VALUES (?, ?, ?, ?)`,
    [booking_id, new_date, new_time, message],
    function (err) {
      if (err) {
        console.log(err);
        return res.status(500).json({ ok: false });
      }
      res.json({ ok: true });
    }
  );
});

// Simple health-check endpoint
app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

// Start server
app.listen(PORT, () => {
  console.log("Glisten backend running on http://localhost:" + PORT);
});
