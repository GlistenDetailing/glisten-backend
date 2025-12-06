const express = require("express");
const cors = require("cors");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");
const nodemailer = require("nodemailer");
const { google } = require("googleapis");
const fetch = require("node-fetch"); // for calling Distance Matrix API + weather APIs
const cron = require("node-cron");   // for daily reminder job

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());

// ------------------ Database setup ------------------

const dbFile = path.join(__dirname, "database.sqlite");
const db = new sqlite3.Database(dbFile);

db.serialize(() => {
  db.run(
    `CREATE TABLE IF NOT EXISTS bookings (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      email TEXT,
      phone TEXT,
      postcode TEXT NOT NULL,
      car_make TEXT,
      car_model TEXT,
      services TEXT NOT NULL,          -- JSON array string
      preferred_date TEXT NOT NULL,    -- "YYYY-MM-DD"
      preferred_time TEXT NOT NULL,    -- "HH:MM"
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )`
  );

  // Add calendar_event_id and reminder_sent columns if they don't exist yet
  db.all("PRAGMA table_info(bookings)", (err, rows) => {
    if (err) {
      console.error("Failed to inspect bookings table:", err);
      return;
    }

    const hasCalendarId = rows.some((r) => r.name === "calendar_event_id");
    if (!hasCalendarId) {
      db.run(
        "ALTER TABLE bookings ADD COLUMN calendar_event_id TEXT",
        (alterErr) => {
          if (alterErr) {
            console.error(
              "Failed to add calendar_event_id column (may already exist):",
              alterErr
            );
          } else {
            console.log("✅ Added calendar_event_id column to bookings table");
          }
        }
      );
    }

    const hasReminderSent = rows.some((r) => r.name === "reminder_sent");
    if (!hasReminderSent) {
      db.run(
        "ALTER TABLE bookings ADD COLUMN reminder_sent INTEGER DEFAULT 0",
        (alterErr) => {
          if (alterErr) {
            console.error(
              "Failed to add reminder_sent column (may already exist):",
              alterErr
            );
          } else {
            console.log("✅ Added reminder_sent column to bookings table");
          }
        }
      );
    }
  });

  // Table for amend / cancel requests
  db.run(
    `CREATE TABLE IF NOT EXISTS amend_requests (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      booking_id INTEGER,
      booking_reference TEXT,
      email TEXT NOT NULL,
      action TEXT NOT NULL,            -- 'amend' or 'cancel'
      message TEXT,
      status TEXT NOT NULL DEFAULT 'pending',  -- 'pending' or 'processed'
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      processed_at TEXT,
      FOREIGN KEY (booking_id) REFERENCES bookings(id)
    )`,
    (err) => {
      if (err) {
        console.error("Failed to create amend_requests table:", err);
      } else {
        console.log("✅ amend_requests table ready");
      }
    }
  );

  // 🔧 Normalise any legacy status values (trim + lowercase)
  db.run(
    "UPDATE bookings SET status = LOWER(TRIM(status)) WHERE status IS NOT NULL",
    (err) => {
      if (err) {
        console.error("Failed to normalise existing booking statuses:", err);
      } else {
        console.log("✅ Normalised existing booking statuses");
      }
    }
  );
});

// sqlite helpers
function dbAll(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

function dbGet(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

function dbRun(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

// ------------------ Email setup ------------------

// Using your info@glistendetailing.co.uk account + app password.
const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: "info@glistendetailing.co.uk",
    pass: "edicqpsqktnnhkxn",
  },
});

// ------------------ Google Calendar setup ------------------

// Calendar that holds your real availability
const GOOGLE_CALENDAR_ID = "bookingsglisten@gmail.com";

// Use service-account JSON from env (prefer base64, fall back to plain JSON)
let googleAuth = null;

function initGoogleAuthFromEnv() {
  try {
    const b64 = process.env.GOOGLE_FIREBASE_CREDENTIALS_B64;
    const raw = process.env.GOOGLE_FIREBASE_CREDENTIALS; // optional fallback for local dev

    let jsonString = null;

    // 1) Try base64-encoded JSON (Render)
    if (b64 && b64.trim().length > 0) {
      jsonString = Buffer.from(b64, "base64").toString("utf8");
    }
    // 2) Fallback: raw JSON in env (for local testing if you ever want it)
    else if (raw && raw.trim().length > 0) {
      jsonString = raw;
    } else {
      console.warn(
        "⚠️ GOOGLE_FIREBASE_CREDENTIALS_B64 / GOOGLE_FIREBASE_CREDENTIALS not set; Google Calendar features are disabled."
      );
      return;
    }

    const creds = JSON.parse(jsonString);
    googleAuth = new google.auth.GoogleAuth({
      credentials: creds,
      scopes: ["https://www.googleapis.com/auth/calendar"],
    });

    console.log(
      `✅ Google Calendar auth initialised for project ${creds.project_id}`
    );
  } catch (err) {
    console.error(
      "❌ Failed to initialise Google Calendar credentials from env:",
      err
    );
  }
}

initGoogleAuthFromEnv();

// ------------------ Google Maps Distance Matrix setup ------------------

const GOOGLE_MAPS_API_KEY =
  process.env.GOOGLE_MAPS_API_KEY || "AIzaSyAzkcG7QzgD0CgzhjW-1er40NZpKQMKk_s";

// Monday–Friday only
function isWorkingDay(dateStr) {
  const d = new Date(dateStr + "T00:00:00");
  const day = d.getDay(); // 0 = Sun, 1 = Mon, ..., 6 = Sat
  return day >= 1 && day <= 5;
}

// ------------------ Shared Calendar helpers ------------------

// Global helper (used by BOTH availability + sync job)
function extractDateAndTimeFromEvent(event) {
  if (!event || !event.start) return null;

  const start = event.start;
  const dateTime =
    start.dateTime || (start.date ? start.date + "T00:00:00" : null);
  if (!dateTime) return null;

  const d = new Date(dateTime);

  const dateStr = d.toISOString().slice(0, 10); // YYYY-MM-DD
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const timeStr = `${hh}:${mm}`;

  return { date: dateStr, time: timeStr };
}

/**
 * Get busy intervals from Google Calendar for a given date.
 * Returns [{ start, end }] in minutes from midnight.
 */
async function getBusyIntervalsForDate(dateStr) {
  if (!googleAuth) {
    // Calendar disabled; treat as no busy intervals.
    return [];
  }

  const dayStart = new Date(dateStr + "T00:00:00");
  const dayEnd = new Date(dateStr + "T23:59:59");

  const timeMin = dayStart.toISOString();
  const timeMax = dayEnd.toISOString();

  const authClient = await googleAuth.getClient();
  const calendar = google.calendar({ version: "v3", auth: authClient });

  const res = await calendar.events.list({
    calendarId: GOOGLE_CALENDAR_ID,
    timeMin,
    timeMax,
    singleEvents: true,
    orderBy: "startTime",
  });

  const events = res.data.items || [];
  const intervals = [];

  for (const ev of events) {
    const start = ev.start;
    const end = ev.end;
    if (!start || !end) continue;

    const startDateTime =
      start.dateTime || (start.date ? start.date + "T00:00:00" : null);
    const endDateTime =
      end.dateTime || (end.date ? end.date + "T23:59:59" : null);
    if (!startDateTime || !endDateTime) continue;

    const startDateObj = new Date(startDateTime);
    const endDateObj = new Date(endDateTime);

    const startMinutes =
      startDateObj.getHours() * 60 + startDateObj.getMinutes();
    const endMinutes =
      endDateObj.getHours() * 60 + endDateObj.getMinutes();

    intervals.push({ start: startMinutes, end: endMinutes });
  }

  return intervals;
}

// ------------------ Email helpers ------------------

// Shared helper to format services list into text
function formatServicesText(servicesValue) {
  try {
    const services = servicesValue
      ? Array.isArray(servicesValue)
        ? servicesValue
        : JSON.parse(servicesValue)
      : [];
    if (!services.length) return "Not specified";

    return services
      .map((s) => {
        const sizePart = s.size ? ` (${s.size})` : "";
        const qty = s.quantity || 1;
        const id = s.serviceId || s.service_id || "service";
        return `${qty}x ${id}${sizePart}`;
      })
      .join(", ");
  } catch {
    return "Not specified";
  }
}

// Email when a request is received (pending)
async function sendBookingReceivedEmail(booking) {
  if (!booking.email) return;

  const bookingNumber = `GL-${String(booking.id).padStart(5, "0")}`;
  const servicesText = formatServicesText(booking.services);

  const textBody = `
Hi ${booking.name || ""},

Thanks for your booking request with Glisten.

We’ve received your request and will review availability based on your location and chosen services. You’ll receive another email once your booking is confirmed or declined.

Booking number: ${bookingNumber}

Details:
- Date: ${booking.preferred_date}
- Time: ${booking.preferred_time}
- Postcode: ${booking.postcode}
- Car: ${booking.car_make || ""} ${booking.car_model || ""}
- Services: ${servicesText}

You’ll be able to amend or cancel this request using your booking number in the Glisten mobile app, or by replying directly to this email.

Kind regards,
Glisten
`.trim();

  const htmlBody = `
  <div style="font-family: Arial, sans-serif; font-size: 14px; color: #222;">
    <div style="margin-bottom: 16px;">
      <img src="cid:glistenLogo"
           alt="Glisten Detailing"
           style="max-width: 240px; height: auto;" />
    </div>

    <p>Hi ${booking.name || ""},</p>

    <p>Thanks for your booking request with <strong>Glisten</strong>.</p>

    <p>
      We’ve received your request and will review availability based on your location
      and chosen services. You’ll receive another email once your booking is
      confirmed or declined.
    </p>

    <p><strong>Booking number:</strong> ${bookingNumber}</p>

    <p><strong>Details:</strong><br/>
      - Date: ${booking.preferred_date}<br/>
      - Time: ${booking.preferred_time}<br/>
      - Postcode: ${booking.postcode}<br/>
      - Car: ${booking.car_make || ""} ${booking.car_model || ""}<br/>
      - Services: ${servicesText}
    </p>

    <p>
      You can amend or cancel this request using your booking number
      in the Glisten mobile app, or by replying directly to this email.
    </p>

    <p>Kind regards,<br/>Glisten</p>
  </div>
  `.trim();

  const mailOptions = {
    from: "info@glistendetailing.co.uk",
    to: booking.email,
    subject: `We’ve received your booking request – ${bookingNumber}`,
    text: textBody,
    html: htmlBody,
    attachments: [
      {
        filename: "logo.png",
        path: path.join(__dirname, "logo.png"),
        cid: "glistenLogo",
      },
    ],
  };

  try {
    await transporter.sendMail(mailOptions);
    console.log(
      `✅ Sent REQUEST RECEIVED email for booking ${booking.id} to ${booking.email}`
    );
  } catch (err) {
    console.error("❌ Failed to send request-received email:", err);
  }
}

// Email when you accept/decline a booking
async function sendBookingDecisionEmail(booking) {
  if (!booking.email) return;

  const bookingNumber = `GL-${String(booking.id).padStart(5, "0")}`;
  const status = booking.status; // "confirmed" or "declined" or "cancelled"
  const servicesText = formatServicesText(booking.services);

  let subject;
  let textBody;
  let htmlBody;

  if (status === "confirmed") {
    subject = `Your booking is confirmed – ${bookingNumber}`;

    textBody = `
Hi ${booking.name || ""},

Good news – your booking with Glisten has been CONFIRMED.

Booking number: ${bookingNumber}

Details:
- Date: ${booking.preferred_date}
- Time: ${booking.preferred_time}
- Postcode: ${booking.postcode}
- Car: ${booking.car_make || ""} ${booking.car_model || ""}
- Services: ${servicesText}

You can amend or cancel this booking using your booking number in the Glisten mobile app, or by replying directly to this email.

Kind regards,
Glisten
`.trim();

    htmlBody = `
    <div style="font-family: Arial, sans-serif; font-size: 14px; color: #222;">
      <div style="margin-bottom: 16px;">
        <img src="cid:glistenLogo"
             alt="Glisten Detailing"
             style="max-width: 240px; height: auto;" />
      </div>

      <p>Hi ${booking.name || ""},</p>

      <p><strong>Good news – your booking with Glisten has been CONFIRMED.</strong></p>

      <p><strong>Booking number:</strong> ${bookingNumber}</p>

      <p><strong>Details:</strong><br/>
        - Date: ${booking.preferred_date}<br/>
        - Time: ${booking.preferred_time}<br/>
        - Postcode: ${booking.postcode}<br/>
        - Car: ${booking.car_make || ""} ${booking.car_model || ""}<br/>
        - Services: ${servicesText}
      </p>

      <p>
        You can amend or cancel this booking using your booking number
        in the Glisten mobile app, or by replying directly to this email.
      </p>

      <p>Kind regards,<br/>Glisten</p>
    </div>
    `.trim();
  } else if (status === "declined") {
    subject = `Your booking request – ${bookingNumber}`;

    textBody = `
Hi ${booking.name || ""},

Thank you for your booking request with Glisten.

Unfortunately, we’re unable to take this booking at the requested time/location.

Booking number: ${bookingNumber}

Details:
- Date: ${booking.preferred_date}
- Time: ${booking.preferred_time}
- Postcode: ${booking.postcode}
- Car: ${booking.car_make || ""} ${booking.car_model || ""}
- Services: ${servicesText}

You can submit a new request in the Glisten mobile app using this booking number as a reference, or reply to this email to discuss alternatives.

Kind regards,
Glisten
`.trim();

    htmlBody = `
    <div style="font-family: Arial, sans-serif; font-size: 14px; color: #222;">
      <div style="margin-bottom: 16px;">
        <img src="cid:glistenLogo"
             alt="Glisten Detailing"
             style="max-width: 240px; height: auto;" />
      </div>

      <p>Hi ${booking.name || ""},</p>

      <p>Thank you for your booking request with <strong>Glisten</strong>.</p>

      <p>
        Unfortunately, we’re unable to take this booking at the requested time/location.
      </p>

      <p><strong>Booking number:</strong> ${bookingNumber}</p>

      <p><strong>Details:</strong><br/>
        - Date: ${booking.preferred_date}<br/>
        - Time: ${booking.preferred_time}<br/>
        - Postcode: ${booking.postcode}<br/>
        - Car: ${booking.car_make || ""} ${booking.car_model || ""}<br/>
        - Services: ${servicesText}
      </p>

      <p>
        You can submit a new request in the Glisten mobile app using this booking number
        as a reference, or reply to this email to discuss alternative options.
      </p>

      <p>Kind regards,<br/>Glisten</p>
    </div>
    `.trim();
  } else if (status === "cancelled") {
    subject = `Your booking has been cancelled – ${bookingNumber}`;

    textBody = `
Hi ${booking.name || ""},

Your booking with Glisten has been cancelled.

Booking number: ${bookingNumber}

If this was a mistake or you’d like to re-book, you can submit a new booking in the Glisten app or reply directly to this email.

Kind regards,
Glisten
`.trim();

    htmlBody = `
    <div style="font-family: Arial, sans-serif; font-size: 14px; color: #222;">
      <div style="margin-bottom: 16px;">
        <img src="cid:glistenLogo"
             alt="Glisten Detailing"
             style="max-width: 240px; height: auto;" />
      </div>

      <p>Hi ${booking.name || ""},</p>

      <p>Your booking with <strong>Glisten</strong> has been <strong>cancelled</strong>.</p>

      <p><strong>Booking number:</strong> ${bookingNumber}</p>

      <p>
        If this was a mistake or you’d like to re-book, you can submit a new booking
        in the Glisten app or reply directly to this email.
      </p>

      <p>Kind regards,<br/>Glisten</p>
    </div>
    `.trim();
  } else {
    // nothing to send for other statuses
    return;
  }

  const mailOptions = {
    from: "info@glistendetailing.co.uk",
    to: booking.email,
    subject,
    text: textBody,
    html: htmlBody,
    attachments: [
      {
        filename: "logo.png",
        path: path.join(__dirname, "logo.png"),
        cid: "glistenLogo",
      },
    ],
  };

  try {
    await transporter.sendMail(mailOptions);
    console.log(
      `✅ Sent DECISION (${status}) email for booking ${booking.id} to ${booking.email}`
    );
  } catch (err) {
    console.error("❌ Failed to send decision email:", err);
  }
}

// Email reminder 1 day before the booking (for confirmed bookings only)
async function sendBookingReminderEmail(booking) {
  if (!booking.email) return;

  const bookingNumber = `GL-${String(booking.id).padStart(5, "0")}`;
  const servicesText = formatServicesText(booking.services);

  const textBody = `
Hi ${booking.name || ""},

This is a quick reminder about your Glisten booking tomorrow.

Booking number: ${bookingNumber}

Details:
- Date: ${booking.preferred_date}
- Time: ${booking.preferred_time}
- Postcode: ${booking.postcode}
- Car: ${booking.car_make || ""} ${booking.car_model || ""}
- Services: ${servicesText}

If you need to amend or cancel, you can use your booking number in the Glisten mobile app, or reply directly to this email.

Kind regards,
Glisten
`.trim();

  const htmlBody = `
  <div style="font-family: Arial, sans-serif; font-size: 14px; color: #222;">
    <div style="margin-bottom: 16px;">
      <img src="cid:glistenLogo"
           alt="Glisten Detailing"
           style="max-width: 240px; height: auto;" />
    </div>

    <p>Hi ${booking.name || ""},</p>

    <p>This is a quick reminder about your <strong>Glisten</strong> booking <strong>tomorrow</strong>.</p>

    <p><strong>Booking number:</strong> ${bookingNumber}</p>

    <p><strong>Details:</strong><br/>
      - Date: ${booking.preferred_date}<br/>
      - Time: ${booking.preferred_time}<br/>
      - Postcode: ${booking.postcode}<br/>
      - Car: ${booking.car_make || ""} ${booking.car_model || ""}<br/>
      - Services: ${servicesText}
    </p>

    <p>
      If you need to amend or cancel, you can use your booking number
      in the Glisten mobile app, or reply directly to this email.
    </p>

    <p>Kind regards,<br/>Glisten</p>
  </div>
  `.trim();

  const mailOptions = {
    from: "info@glistendetailing.co.uk",
    to: booking.email,
    subject: `Reminder: your Glisten booking tomorrow – ${bookingNumber}`,
    text: textBody,
    html: htmlBody,
    attachments: [
      {
        filename: "logo.png",
        path: path.join(__dirname, "logo.png"),
        cid: "glistenLogo",
      },
    ],
  };

  try {
    await transporter.sendMail(mailOptions);
    console.log(
      `✅ Sent REMINDER email for booking ${booking.id} to ${booking.email}`
    );
  } catch (err) {
    console.error("❌ Failed to send reminder email:", err);
  }
}

// ------------------ Time + postcode logic ------------------

// Working hours: 08:30–16:30
const WORK_START = 8 * 60 + 30;
const WORK_END = 16 * 60 + 30;

// Max travel time between jobs (minutes)
const TRAVEL_LIMIT_MIN = 20;

// EXTRA BUFFER AFTER EACH JOB (minutes)
const BUFFER_AFTER_JOB_MIN = 30;

// Slot step for searching availability
const SLOT_STEP_MIN = 15;

function timeStringToMinutes(timeStr) {
  const [h, m] = timeStr.split(":").map(Number);
  return h * 60 + m;
}

function minutesToTimeString(mins) {
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
}

function normalizePostcode(postcode) {
  if (!postcode) return "";
  return postcode.trim().toUpperCase().replace(/\s+/g, "");
}

/**
 * REAL driving-time check using Google Distance Matrix API (mode=driving).
 * Returns minutes; on any error falls back to a large value so it's treated as "too far".
 */
async function getTravelTimeMinutes(fromPostcode, toPostcode) {
  try {
    if (!fromPostcode || !toPostcode) return 999;

    const origins = encodeURIComponent(fromPostcode.trim());
    const destinations = encodeURIComponent(toPostcode.trim());

    const url = `https://maps.googleapis.com/maps/api/distancematrix/json?units=metric&origins=${origins}&destinations=${destinations}&mode=driving&key=${GOOGLE_MAPS_API_KEY}`;

    const res = await fetch(url);
    if (!res.ok) {
      console.error("Distance Matrix HTTP error:", res.status, res.statusText);
      return 999;
    }

    const data = await res.json();

    if (
      data.status !== "OK" ||
      !data.rows ||
      !data.rows[0] ||
      !data.rows[0].elements[0]
    ) {
      console.error(
        "Distance Matrix bad response:",
        data.status,
        data.error_message
      );
      return 999;
    }

    const element = data.rows[0].elements[0];
    if (
      element.status !== "OK" ||
      !element.duration ||
      typeof element.duration.value !== "number"
    ) {
      console.error("Distance Matrix element not OK:", element.status);
      return 999;
    }

    const seconds = element.duration.value;
    const minutes = Math.round(seconds / 60);
    return minutes;
  } catch (err) {
    console.error("Failed to call Distance Matrix API:", err);
    return 999; // treat as "too far" on error
  }
}

// ------------------ Service duration logic ------------------

function getServiceDurationMinutes(service) {
  const id = (service.serviceId || "").toLowerCase();
  const size = (service.size || "").toLowerCase();
  const quantity = service.quantity || 1;

  let singleDuration;

  if (id === "basic_wash") {
    singleDuration = 60;
  } else if (id === "exterior_detailed") {
    if (size === "small") singleDuration = 60;
    else if (size === "medium") singleDuration = 75;
    else if (size === "large") singleDuration = 90;
    else singleDuration = 60;
  } else if (id === "full_detailed") {
    if (size === "small") singleDuration = 75;
    else if (size === "medium") singleDuration = 95;
    else if (size === "large") singleDuration = 120;
    else singleDuration = 75;
  } else {
    singleDuration = 60;
  }

  return singleDuration * quantity;
}

function estimateBookingDurationMinutes(services) {
  if (!Array.isArray(services) || services.length === 0) {
    return 60;
  }

  let total = 0;
  for (const s of services) {
    total += getServiceDurationMinutes(s);
  }
  return total;
}

// ------------------ Weather helpers (heavy rain warning only) ------------------

/**
 * Geocode a postcode to { lat, lng } using Google Maps Geocoding API.
 * Returns null on any error.
 */
async function geocodePostcode(postcode) {
  try {
    if (!postcode) return null;
    const addr = encodeURIComponent(postcode.trim());
    const url = `https://maps.googleapis.com/maps/api/geocode/json?address=${addr}&key=${GOOGLE_MAPS_API_KEY}`;

    const res = await fetch(url);
    if (!res.ok) {
      console.error("Geocoding HTTP error:", res.status, res.statusText);
      return null;
    }

    const data = await res.json();
    if (data.status !== "OK" || !data.results || !data.results[0]) {
      console.error("Geocoding bad response:", data.status, data.error_message);
      return null;
    }

    const loc = data.results[0].geometry.location;
    if (
      !loc ||
      typeof loc.lat !== "number" ||
      typeof loc.lng !== "number"
    ) {
      return null;
    }

    return { lat: loc.lat, lng: loc.lng };
  } catch (err) {
    console.error("Failed to call Geocoding API:", err);
    return null;
  }
}

/**
 * Get total daily precipitation (mm) for a given date/postcode using Open-Meteo.
 * Returns { precipitationMm, sourceAvailable }.
 */
async function getDailyPrecipitationMm(dateStr, postcode) {
  try {
    const geo = await geocodePostcode(postcode);
    if (!geo) {
      return { precipitationMm: null, sourceAvailable: false };
    }

    const { lat, lng } = geo;
    const url = `https://api.open-meteo.com/v1/forecast?latitude=${encodeURIComponent(
      lat
    )}&longitude=${encodeURIComponent(
      lng
    )}&daily=precipitation_sum&timezone=Europe%2FLondon&start_date=${encodeURIComponent(
      dateStr
    )}&end_date=${encodeURIComponent(dateStr)}`;

    const res = await fetch(url);
    if (!res.ok) {
      console.error("Open-Meteo HTTP error:", res.status, res.statusText);
      return { precipitationMm: null, sourceAvailable: false };
    }

    const data = await res.json();
    if (
      !data.daily ||
      !Array.isArray(data.daily.precipitation_sum) ||
      data.daily.precipitation_sum.length === 0
    ) {
      console.error("Open-Meteo: missing daily precipitation_sum");
      return { precipitationMm: null, sourceAvailable: false };
    }

    const mm = Number(data.daily.precipitation_sum[0]);
    if (!isFinite(mm)) {
      return { precipitationMm: null, sourceAvailable: false };
    }

    return { precipitationMm: mm, sourceAvailable: true };
  } catch (err) {
    console.error("Failed to call Open-Meteo API:", err);
    return { precipitationMm: null, sourceAvailable: false };
  }
}

// ------------------ Calendar event helpers ------------------

async function syncConfirmedBookingsFromCalendar() {
  if (!googleAuth) {
    console.warn(
      "⚠️ Google Calendar not configured; skipping calendar→booking sync."
    );
    return 0;
  }

  try {
    const authClient = await googleAuth.getClient();
    const calendar = google.calendar({ version: "v3", auth: authClient });

    const rows = await dbAll(
      `SELECT id, calendar_event_id, preferred_date, preferred_time
         FROM bookings
        WHERE status = 'confirmed'
          AND calendar_event_id IS NOT NULL`
    );

    let updatedCount = 0;

    for (const row of rows) {
      if (!row.calendar_event_id) continue;

      try {
        const res = await calendar.events.get({
          calendarId: GOOGLE_CALENDAR_ID,
          eventId: row.calendar_event_id,
        });

        const event = res.data;
        const dt = extractDateAndTimeFromEvent(event);
        if (!dt) continue;

        const newDate = dt.date;
        const newTime = dt.time;
        const oldDate = row.preferred_date;
        const oldTime = row.preferred_time;

        if (newDate !== oldDate || newTime !== oldTime) {
          await dbRun(
            `UPDATE bookings
               SET preferred_date = ?, preferred_time = ?
             WHERE id = ?`,
            [newDate, newTime, row.id]
          );

          console.log(
            `🔄 Synced booking ${row.id} from Calendar: ${oldDate} ${oldTime} -> ${newDate} ${newTime}`
          );
          updatedCount++;
        }
      } catch (err) {
        if (err && err.code === 404) {
          console.warn(
            `⚠️ Calendar event ${row.calendar_event_id} for booking ${row.id} not found; leaving booking unchanged.`
          );
        } else {
          console.error(
            `❌ Failed to sync booking ${row.id} from Calendar:`,
            err.message || err
          );
        }
      }
    }

    return updatedCount;
  } catch (err) {
    console.error("❌ Calendar→booking sync job failed:", err);
    return 0;
  }
}

async function createOrReplaceCalendarEventForBooking(booking) {
  try {
    if (!booking || booking.status !== "confirmed") return;
    if (!booking.preferred_date || !booking.preferred_time) return;
    if (!googleAuth) {
      console.warn(
        "⚠️ Google Calendar not configured; skipping event creation."
      );
      return;
    }

    const servicesArray = booking.services
      ? Array.isArray(booking.services)
        ? booking.services
        : JSON.parse(booking.services)
      : [];

    const durationMinutes = estimateBookingDurationMinutes(servicesArray);
    const [hourStr, minuteStr] = booking.preferred_time.split(":");
    const startDateTime = new Date(
      `${booking.preferred_date}T${hourStr.padStart(2, "0")}:${minuteStr.padStart(
        2,
        "0"
      )}:00`
    );
    const endDateTime = new Date(
      startDateTime.getTime() + durationMinutes * 60 * 1000
    );

    const servicesText = (() => {
      try {
        const services = servicesArray;
        if (!services.length) return "Not specified";

        return services
          .map((s) => {
            const sizePart = s.size ? ` (${s.size})` : "";
            const qty = s.quantity || 1;
            const id = s.serviceId || s.service_id || "service";
            return `${qty}x ${id}${sizePart}`;
          })
          .join(", ");
      } catch {
        return "Not specified";
      }
    })();

    const bookingNumber = `GL-${String(booking.id).padStart(5, "0")}`;

    const authClient = await googleAuth.getClient();
    const calendar = google.calendar({ version: "v3", auth: authClient });

    const summary = `Detail – ${booking.name || "Customer"} (${booking.postcode})`;
    const description = [
      `Booking number: ${bookingNumber}`,
      "",
      `Name: ${booking.name || ""}`,
      `Phone: ${booking.phone || ""}`,
      `Email: ${booking.email || ""}`,
      `Postcode: ${booking.postcode}`,
      `Car: ${booking.car_make || ""} ${booking.car_model || ""}`,
      `Services: ${servicesText}`,
    ].join("\n");

    // If there's already an event, delete it and recreate cleanly
    if (booking.calendar_event_id) {
      try {
        await calendar.events.delete({
          calendarId: GOOGLE_CALENDAR_ID,
          eventId: booking.calendar_event_id,
        });
        console.log(
          `ℹ️ Deleted old calendar event ${booking.calendar_event_id} for booking ${booking.id}`
        );
      } catch (err) {
        console.error(
          `⚠️ Failed to delete old calendar event ${booking.calendar_event_id}:`,
          err.message || err
        );
      }
    }

    const res = await calendar.events.insert({
      calendarId: GOOGLE_CALENDAR_ID,
      requestBody: {
        summary,
        description,
        location: booking.postcode || "",
        start: {
          dateTime: startDateTime.toISOString(),
          timeZone: "Europe/London",
        },
        end: {
          dateTime: endDateTime.toISOString(),
          timeZone: "Europe/London",
        },
      },
    });

    const eventId = res.data.id;
    if (eventId) {
      await dbRun(
        "UPDATE bookings SET calendar_event_id = ? WHERE id = ?",
        [eventId, booking.id]
      );
      console.log(
        `✅ Created calendar event ${eventId} for booking ${booking.id}`
      );
    } else {
      console.log(
        `⚠️ Calendar insert returned no event ID for booking ${booking.id}`
      );
    }
  } catch (err) {
    console.error(
      "❌ Failed to create/replace Google Calendar event for booking:",
      err
    );
  }
}

async function deleteCalendarEventForBooking(booking) {
  try {
    if (!booking || !booking.calendar_event_id) return;
    if (!googleAuth) {
      console.warn(
        "⚠️ Google Calendar not configured; skipping event deletion."
      );
      return;
    }

    const authClient = await googleAuth.getClient();
    const calendar = google.calendar({ version: "v3", auth: authClient });

    await calendar.events.delete({
      calendarId: GOOGLE_CALENDAR_ID,
      eventId: booking.calendar_event_id,
    });

    await dbRun(
      "UPDATE bookings SET calendar_event_id = NULL WHERE id = ?",
      [booking.id]
    );

    console.log(
      `✅ Deleted calendar event ${booking.calendar_event_id} for booking ${booking.id}`
    );
  } catch (err) {
    console.error(
      "❌ Failed to delete calendar event for booking:",
      err.message || err
    );
  }
}

// ------------------ Reminder processing helpers ------------------

function parseSqliteDateTime(ts) {
  if (!ts) return null;
  // SQLite CURRENT_TIMESTAMP is "YYYY-MM-DD HH:MM:SS"
  // Replace the space with "T" so JS can parse it as local time.
  return new Date(ts.replace(" ", "T"));
}

/**
 * Find confirmed bookings for "tomorrow" and send reminder emails
 * if they were booked more than 24 hours before the appointment.
 */
async function processBookingReminders() {
  const now = new Date();

  // Compute "tomorrow" in the server's local timezone
  const tomorrow = new Date(
    now.getFullYear(),
    now.getMonth(),
    now.getDate() + 1
  );
  const year = tomorrow.getFullYear();
  const month = String(tomorrow.getMonth() + 1).padStart(2, "0");
  const day = String(tomorrow.getDate()).padStart(2, "0");
  const tomorrowStr = `${year}-${month}-${day}`;

  const rows = await dbAll(
    `SELECT * FROM bookings
     WHERE preferred_date = ?
       AND status = 'confirmed'
       AND email IS NOT NULL
       AND email != ''
       AND (reminder_sent IS NULL OR reminder_sent = 0)`,
    [tomorrowStr]
  );

  let sentCount = 0;

  for (const row of rows) {
    const createdAt = parseSqliteDateTime(row.created_at);
    if (!createdAt || isNaN(createdAt.getTime())) {
      continue;
    }

    const bookingStart = new Date(
      `${row.preferred_date}T${row.preferred_time}:00`
    );

    // Skip if the start time is already in the past for some reason
    if (bookingStart <= now) continue;

    const diffMs = bookingStart.getTime() - createdAt.getTime();
    const diffHours = diffMs / (1000 * 60 * 60);

    // Only send if they booked more than 24 hours in advance
    if (diffHours <= 24) continue;

    await sendBookingReminderEmail(row);
    await dbRun("UPDATE bookings SET reminder_sent = 1 WHERE id = ?", [
      row.id,
    ]);
    sentCount++;
  }

  return sentCount;
}

// ------------------ Core validation logic ------------------

async function validateBooking({ date, postcode, services, preferred_time }) {
  if (!date || !postcode || !preferred_time) {
    throw {
      code: 400,
      error: "MISSING_FIELDS",
      message: "date, postcode and time are required.",
    };
  }

  // Block weekends completely
  if (!isWorkingDay(date)) {
    throw {
      code: 400,
      error: "OUTSIDE_WORKING_DAYS",
      message: "We only take bookings Monday to Friday.",
    };
  }

  const servicesArray = Array.isArray(services) ? services : [];
  const baseDuration = estimateBookingDurationMinutes(servicesArray);
  const requestedDuration = baseDuration + BUFFER_AFTER_JOB_MIN; // add 30-min buffer
  const requestedStart = timeStringToMinutes(preferred_time);
  const requestedEnd = requestedStart + requestedDuration;

  if (requestedStart < WORK_START || requestedEnd > WORK_END) {
    throw {
      code: 400,
      error: "OUTSIDE_WORKING_HOURS",
      message: "Selected time is outside working hours.",
    };
  }

  // Existing bookings on that date (pending + confirmed)
  const rows = await dbAll(
    `SELECT * FROM bookings
     WHERE preferred_date = ?
       AND status IN ('pending', 'confirmed')
     ORDER BY preferred_time ASC`,
    [date]
  );

  // Calendar busy intervals for that date
  let calendarBusy = [];
  try {
    calendarBusy = await getBusyIntervalsForDate(date);
  } catch (err) {
    console.error("Failed to load Google Calendar intervals:", err);
    // On calendar error, we just ignore it and continue with bookings only
  }

  // If all existing bookings that day are in zones more than 20 minutes from this
  // postcode, then we consider this a "different area for that day".
  if (rows.length > 0) {
    let allFar = true;
    for (const row of rows) {
      const travel = await getTravelTimeMinutes(postcode, row.postcode);
      if (travel <= TRAVEL_LIMIT_MIN) {
        allFar = false;
        break;
      }
    }
    if (allFar) {
      throw {
        code: 400,
        error: "OUT_OF_AREA_FOR_DAY",
        message:
          "We’re already booked in another area for that day (more than 20 minutes away).",
      };
    }
  }

  // Build intervals from bookings + calendar
  const intervals = [];

  // bookings (duration + 30-min buffer)
  for (const row of rows) {
    const svc = row.services ? JSON.parse(row.services) : [];
    const dur = estimateBookingDurationMinutes(svc) + BUFFER_AFTER_JOB_MIN;
    const start = timeStringToMinutes(row.preferred_time);
    const end = start + dur;
    intervals.push({ start, end, postcode: row.postcode });
  }

  // calendar busy (no postcode needed, no extra buffer)
  for (const c of calendarBusy) {
    intervals.push({ start: c.start, end: c.end, postcode: null });
  }

  // 1) Disallow overlaps with existing bookings or calendar blocks
  for (const i of intervals) {
    const latestStart = Math.max(requestedStart, i.start);
    const earliestEnd = Math.min(requestedEnd, i.end);
    if (latestStart < earliestEnd) {
      throw {
        code: 400,
        error: "TIME_TAKEN",
        message: "That time is no longer available.",
      };
    }
  }

  // 2) Travel-time checks only care about real bookings (with postcode)
  const bookingIntervals = intervals.filter((i) => i.postcode);

  const prev = bookingIntervals
    .filter((i) => i.end <= requestedStart)
    .sort((a, b) => b.end - a.end)[0];

  const next = bookingIntervals
    .filter((i) => i.start >= requestedEnd)
    .sort((a, b) => a.start - b.start)[0];

  if (prev) {
    const mins = await getTravelTimeMinutes(prev.postcode, postcode);
    if (mins > TRAVEL_LIMIT_MIN) {
      throw {
        code: 400,
        error: "TRAVEL_TOO_FAR",
        message: "There isn’t enough travel time from the previous job.",
      };
    }
  }

  if (next) {
    const mins = await getTravelTimeMinutes(postcode, next.postcode);
    if (mins > TRAVEL_LIMIT_MIN) {
      throw {
        code: 400,
        error: "TRAVEL_TOO_FAR",
        message: "There isn’t enough travel time to the next job.",
      };
    }
  }
}

// ------------------ Routes ------------------

app.get("/", (req, res) => {
  res.json({ ok: true, message: "Glisten backend running" });
});

// Get all bookings (admin)
app.get("/api/bookings", async (req, res) => {
  try {
    const rows = await dbAll(
      `SELECT * FROM bookings
       ORDER BY preferred_date DESC, preferred_time DESC`
    );

    const mapped = rows.map((row) => ({
      ...row,
      services: row.services ? JSON.parse(row.services) : [],
    }));

    res.json(mapped);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to fetch bookings" });
  }
});

// Admin: update booking details (and resync calendar if needed)
app.patch("/api/bookings/:id", async (req, res) => {
  try {
    const bookingId = req.params.id;

    let {
      name,
      email,
      phone,
      postcode,
      car_make,
      car_model,
      services,
      preferred_date,
      preferred_time,
      status,
    } = req.body || {};

    const servicesJson =
      Array.isArray(services) ? JSON.stringify(services) : null;

    // 🔧 normalise and validate status if provided
    let normalizedStatus = null;
    if (typeof status === "string") {
      const trimmed = status.trim().toLowerCase();
      const allowedStatuses = ["pending", "confirmed", "declined", "cancelled"];
      if (allowedStatuses.includes(trimmed)) {
        normalizedStatus = trimmed;
      } else {
        // invalid status payload → ignore status change
        normalizedStatus = null;
      }
    }

    const result = await dbRun(
      `UPDATE bookings
         SET
           name           = COALESCE(?, name),
           email          = COALESCE(?, email),
           phone          = COALESCE(?, phone),
           postcode       = COALESCE(?, postcode),
           car_make       = COALESCE(?, car_make),
           car_model      = COALESCE(?, car_model),
           services       = COALESCE(?, services),
           preferred_date = COALESCE(?, preferred_date),
           preferred_time = COALESCE(?, preferred_time),
           status         = COALESCE(?, status)
       WHERE id = ?`,
      [
        name ?? null,
        email ?? null,
        phone ?? null,
        postcode ?? null,
        car_make ?? null,
        car_model ?? null,
        servicesJson,
        preferred_date ?? null,
        preferred_time ?? null,
        normalizedStatus,
        bookingId,
      ]
    );

    if (result.changes === 0) {
      return res.status(404).json({ error: "Booking not found" });
    }

    const updated = await dbGet(
      `SELECT * FROM bookings WHERE id = ?`,
      [bookingId]
    );

    if (!updated) {
      return res.status(404).json({ error: "Booking not found" });
    }

    updated.services = updated.services ? JSON.parse(updated.services) : [];

    if (updated.status === "confirmed") {
      await createOrReplaceCalendarEventForBooking(updated);
    } else if (["declined", "cancelled"].includes(updated.status)) {
      await deleteCalendarEventForBooking(updated);
    }

    res.json(updated);
  } catch (err) {
    console.error("Failed to update booking:", err);
    res.status(500).json({ error: "Failed to update booking" });
  }
});

// Get single booking
app.get("/api/bookings/:id", async (req, res) => {
  try {
    const booking = await dbGet(
      `SELECT * FROM bookings WHERE id = ?`,
      [req.params.id]
    );

    if (!booking) {
      return res.status(404).json({ error: "Booking not found" });
    }

    booking.services = booking.services ? JSON.parse(booking.services) : [];
    res.json(booking);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to fetch booking" });
  }
});

// Create booking
app.post("/api/bookings", async (req, res) => {
  try {
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
      // device_token
    } = req.body;

    if (!name || !postcode || !preferred_date || !preferred_time) {
      return res.status(400).json({
        error: "MISSING_FIELDS",
        message: "Name, postcode, date and time are required.",
      });
    }

    const servicesArray = Array.isArray(services) ? services : [];

    await validateBooking({
      date: preferred_date,
      postcode,
      services: servicesArray,
      preferred_time,
    });

    const result = await dbRun(
      `INSERT INTO bookings
       (name, email, phone, postcode, car_make, car_model, services, preferred_date, preferred_time)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        name,
        email || null,
        phone || null,
        postcode,
        car_make || null,
        car_model || null,
        JSON.stringify(servicesArray),
        preferred_date,
        preferred_time,
      ]
    );

    const newBooking = await dbGet(
      `SELECT * FROM bookings WHERE id = ?`,
      [result.lastID]
    );
    if (newBooking) {
      newBooking.services = newBooking.services
        ? JSON.parse(newBooking.services)
        : [];
      sendBookingReceivedEmail(newBooking);
    }

    res.json({ booking_id: result.lastID });
  } catch (err) {
    console.error(err);
    if (err && err.code) {
      return res.status(err.code).json({
        error: err.error || "BOOKING_FAILED",
        message: err.message || "Failed to create booking.",
      });
    }
    res.status(500).json({ error: "Failed to create booking" });
  }
});

// Update booking status (confirm / decline / cancel)
app.patch("/api/bookings/:id/status", async (req, res) => {
  try {
    const bookingId = req.params.id;
    let { status } = req.body || {};

    if (typeof status === "string") {
      status = status.trim().toLowerCase();
    }

    const allowedStatuses = ["pending", "confirmed", "declined", "cancelled"];
    if (!allowedStatuses.includes(status)) {
      return res.status(400).json({ error: "Invalid status" });
    }

    const result = await dbRun(
      `UPDATE bookings SET status = ? WHERE id = ?`,
      [status, bookingId]
    );

    if (result.changes === 0) {
      return res.status(404).json({ error: "Booking not found" });
    }

    const updated = await dbGet(
      `SELECT * FROM bookings WHERE id = ?`,
      [bookingId]
    );

    if (!updated) {
      return res.status(404).json({ error: "Booking not found" });
    }

    updated.services = updated.services ? JSON.parse(updated.services) : [];

    sendBookingDecisionEmail(updated);

    if (updated.status === "confirmed") {
      createOrReplaceCalendarEventForBooking(updated);
    } else if (["declined", "cancelled"].includes(updated.status)) {
      deleteCalendarEventForBooking(updated);
    }

    res.json(updated);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to update booking status" });
  }
});

// Clear history (accepted & declined)
app.delete("/api/bookings/history", async (req, res) => {
  try {
    const result = await dbRun(
      `DELETE FROM bookings WHERE status IN ('confirmed', 'declined')`
    );

    res.json({
      deleted: result.changes || 0,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to clear booking history" });
  }
});

// Time-slot availability for a specific date
app.post("/api/availability", async (req, res) => {
  try {
    const { date, postcode, services } = req.body;

    if (!date || !postcode) {
      return res
        .status(400)
        .json({ error: "date and postcode are required" });
    }

    if (!isWorkingDay(date)) {
      return res.json({ date, slots: [] });
    }

    const servicesArray = Array.isArray(services) ? services : [];
    const baseDuration = estimateBookingDurationMinutes(servicesArray);
    const requestedDuration = baseDuration + BUFFER_AFTER_JOB_MIN;

    const rows = await dbAll(
      `SELECT * FROM bookings
       WHERE preferred_date = ?
         AND status IN ('pending', 'confirmed')
       ORDER BY preferred_time ASC`,
      [date]
    );

    if (rows.length > 0) {
      let allFar = true;
      for (const row of rows) {
        const travel = await getTravelTimeMinutes(postcode, row.postcode);
        if (travel <= TRAVEL_LIMIT_MIN) {
          allFar = false;
          break;
        }
      }
      if (allFar) {
        return res.json({ date, slots: [] });
      }
    }

    let calendarBusy = [];
    try {
      calendarBusy = await getBusyIntervalsForDate(date);
    } catch (err) {
      console.error("Failed to load Google Calendar intervals:", err);
    }

    const intervals = [];

    for (const row of rows) {
      const svc = row.services ? JSON.parse(row.services) : [];
      const dur = estimateBookingDurationMinutes(svc) + BUFFER_AFTER_JOB_MIN;
      const start = timeStringToMinutes(row.preferred_time);
      const end = start + dur;
      intervals.push({ start, end, postcode: row.postcode });
    }

    for (const c of calendarBusy) {
      intervals.push({ start: c.start, end: c.end, postcode: null });
    }

    const possibleSlots = [];

    for (
      let candidateStart = WORK_START;
      candidateStart + requestedDuration <= WORK_END;
      candidateStart += SLOT_STEP_MIN
    ) {
      const candidateEnd = candidateStart + requestedDuration;

      let overlaps = false;
      for (const i of intervals) {
        const latestStart = Math.max(candidateStart, i.start);
        const earliestEnd = Math.min(candidateEnd, i.end);
        if (latestStart < earliestEnd) {
          overlaps = true;
          break;
        }
      }
      if (overlaps) continue;

      let travelOk = true;

      const bookingIntervals = intervals.filter((i) => i.postcode);

      const prev = bookingIntervals
        .filter((i) => i.end <= candidateStart)
        .sort((a, b) => b.end - a.end)[0];

      const next = bookingIntervals
        .filter((i) => i.start >= candidateEnd)
        .sort((a, b) => a.start - b.start)[0];

      if (prev) {
        const mins = await getTravelTimeMinutes(prev.postcode, postcode);
        if (mins > TRAVEL_LIMIT_MIN) travelOk = false;
      }
      if (next && travelOk) {
        const mins = await getTravelTimeMinutes(postcode, next.postcode);
        if (mins > TRAVEL_LIMIT_MIN) travelOk = false;
      }

      if (!travelOk) continue;

      possibleSlots.push(minutesToTimeString(candidateStart));
    }

    res.json({ date, slots: possibleSlots });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to calculate availability" });
  }
});

// Date-level availability for a postcode (for greying out calendar days)
app.post("/api/date-availability", async (req, res) => {
  try {
    const { postcode, start_date, end_date } = req.body;

    if (!postcode || !start_date || !end_date) {
      return res.status(400).json({
        error: "MISSING_FIELDS",
        message: "postcode, start_date and end_date are required.",
      });
    }

    const normPostcode = normalizePostcode(postcode);

    const rows = await dbAll(
      `SELECT * FROM bookings
       WHERE preferred_date BETWEEN ? AND ?
         AND status IN ('pending', 'confirmed')`,
      [start_date, end_date]
    );

    const byDate = {};
    for (const row of rows) {
      if (!byDate[row.preferred_date]) {
        byDate[row.preferred_date] = [];
      }
      byDate[row.preferred_date].push(row);
    }

    const dates = [];
    const start = new Date(start_date + "T00:00:00");
    const end = new Date(end_date + "T00:00:00");

    for (
      let d = new Date(start.getTime());
      d <= end;
      d.setDate(d.getDate() + 1)
    ) {
      const dStr = d.toISOString().slice(0, 10);
      const bookingsForDay = byDate[dStr] || [];

      let inArea = true;

      if (!isWorkingDay(dStr)) {
        inArea = false;
      } else if (bookingsForDay.length > 0) {
        let allFar = true;
        for (const b of bookingsForDay) {
          const travel = await getTravelTimeMinutes(normPostcode, b.postcode);
          if (travel <= TRAVEL_LIMIT_MIN) {
            allFar = false;
            break;
          }
        }
        inArea = !allFar;
      } else {
        inArea = true;
      }

      dates.push({
        date: dStr,
        in_area: inArea,
      });
    }

    res.json({
      postcode: normPostcode,
      start_date,
      end_date,
      dates,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to calculate date availability" });
  }
});

// ------------------ Amend / Cancel endpoints ------------------

app.post("/api/amend-booking", async (req, res) => {
  try {
    const { booking_reference, email, action, message } = req.body || {};

    if (!booking_reference || !email || !action) {
      return res.status(400).json({
        error: "MISSING_FIELDS",
        message: "booking_reference, email and action are required.",
      });
    }

    const actionLower = String(action).toLowerCase();
    if (!["amend", "cancel"].includes(actionLower)) {
      return res.status(400).json({
        error: "INVALID_ACTION",
        message: "action must be 'amend' or 'cancel'.",
      });
    }

    let bookingId = null;
    const ref = String(booking_reference).trim().toUpperCase();

    if (ref.startsWith("GL-")) {
      const numStr = ref.slice(3);
      const idNum = parseInt(numStr, 10);
      if (!isNaN(idNum) && idNum > 0) {
        const booking = await dbGet(
          "SELECT id FROM bookings WHERE id = ?",
          [idNum]
        );
        if (booking) {
          bookingId = booking.id;
        }
      }
    }

    const result = await dbRun(
      `INSERT INTO amend_requests
       (booking_id, booking_reference, email, action, message, status)
       VALUES (?, ?, ?, ?, ?, 'pending')`,
      [
        bookingId,
        booking_reference,
        email,
        actionLower,
        message || null,
      ]
    );

    res.json({
      ok: true,
      id: result.lastID,
      message:
        "Your request has been received. We’ll review it and get back to you.",
    });
  } catch (err) {
    console.error("Failed to create amend request:", err);
    res.status(500).json({ error: "Failed to create amend request" });
  }
});

app.get("/api/amend-requests", async (req, res) => {
  try {
    const rows = await dbAll(
      `SELECT * FROM amend_requests
       ORDER BY created_at DESC`
    );
    res.json(rows);
  } catch (err) {
    console.error("Failed to fetch amend requests:", err);
    res.status(500).json({ error: "Failed to fetch amend requests" });
  }
});

app.patch("/api/amend-requests/:id/status", async (req, res) => {
  try {
    const id = req.params.id;
    const { status } = req.body || {};

    const allowed = ["pending", "processed"];
    if (!allowed.includes(status)) {
      return res.status(400).json({ error: "Invalid status" });
    }

    let sql;
    if (status === "processed") {
      sql = `UPDATE amend_requests
             SET status = ?, processed_at = CURRENT_TIMESTAMP
             WHERE id = ?`;
    } else {
      sql = `UPDATE amend_requests
             SET status = ?, processed_at = NULL
             WHERE id = ?`;
    }

    const result = await dbRun(sql, [status, id]);

    if (result.changes === 0) {
      return res.status(404).json({ error: "Amend request not found" });
    }

    const updated = await dbGet(
      `SELECT * FROM amend_requests WHERE id = ?`,
      [id]
    );

    res.json(updated);
  } catch (err) {
    console.error("Failed to update amend request status:", err);
    res.status(500).json({ error: "Failed to update amend request status" });
  }
});

// ------------------ Reminder trigger endpoint ------------------

app.post("/api/send-reminders", async (req, res) => {
  const secretHeader = req.headers["x-reminder-secret"];
  const expectedSecret = process.env.REMINDER_SECRET;

  if (!expectedSecret) {
    console.warn(
      "⚠️ REMINDER_SECRET is not set; /api/send-reminders is effectively disabled."
    );
    return res
      .status(500)
      .json({ error: "REMINDER_SECRET not configured on the server" });
  }

  if (!secretHeader || secretHeader !== expectedSecret) {
    return res.status(403).json({ error: "Forbidden" });
  }

  try {
    const sentCount = await processBookingReminders();
    res.json({ ok: true, reminders_sent: sentCount });
  } catch (err) {
    console.error("Failed to process reminders:", err);
    res.status(500).json({ error: "Failed to process reminders" });
  }
});

// ------------------ Calendar sync endpoint (for Admin Refresh) ------------------

app.post("/api/sync-calendar", async (req, res) => {
  try {
    const updated = await syncConfirmedBookingsFromCalendar();
    res.json({ ok: true, updated });
  } catch (err) {
    console.error("Failed to sync from calendar:", err);
    res.status(500).json({ error: "Failed to sync from calendar" });
  }
});

// ------------------ NEW: Weather-check endpoint (heavy rain warning only) ------------------

app.post("/api/weather-check", async (req, res) => {
  try {
    const { date, postcode } = req.body || {};

    if (!date || !postcode) {
      return res.status(400).json({
        error: "MISSING_FIELDS",
        message: "date and postcode are required.",
      });
    }

    const normPostcode = normalizePostcode(postcode);

    // Only forecast on working days; weekends are irrelevant for bookings
    if (!isWorkingDay(date)) {
      return res.json({
        date,
        postcode: normPostcode,
        heavy_rain: false,
        precipitation_mm: null,
        source_available: true,
      });
    }

    const { precipitationMm, sourceAvailable } =
      await getDailyPrecipitationMm(date, normPostcode);

    // If we couldn't reach the weather source, return "no warning"
    if (!sourceAvailable || precipitationMm == null) {
      return res.json({
        date,
        postcode: normPostcode,
        heavy_rain: false,
        precipitation_mm: null,
        source_available: false,
      });
    }

    // Simple threshold: treat >= 8mm of rain as "heavy rain" for warning purposes
    const HEAVY_RAIN_THRESHOLD_MM = 4;
    const heavyRain = precipitationMm >= HEAVY_RAIN_THRESHOLD_MM;

    res.json({
      date,
      postcode: normPostcode,
      heavy_rain: heavyRain,
      precipitation_mm: precipitationMm,
      source_available: true,
    });
  } catch (err) {
    console.error("Failed to process weather-check:", err);
    res.status(500).json({
      error: "WEATHER_CHECK_FAILED",
      message: "Failed to check weather for this date.",
    });
  }
});

// ------------------ Cron jobs ------------------

// Daily reminder at 09:00
cron.schedule(
  "0 9 * * *",
  async () => {
    console.log("⏰ Running daily reminder check (cron)...");
    try {
      const sentCount = await processBookingReminders();
      console.log(`✅ Reminder job complete – sent ${sentCount} reminders.`);
    } catch (err) {
      console.error("❌ Reminder cron job failed:", err);
    }
  },
  { timezone: "Europe/London" }
);

// Calendar → Booking sync every 1 minute (DISABLED)
//
// This used to keep bookings in sync when events were manually moved in Google
// Calendar. That behaviour is no longer supported because it caused
// unpredictable changes. The cron job is now intentionally disabled. You can
// still trigger a manual sync via the /api/sync-calendar endpoint if needed.
//
// cron.schedule("*/1 * * * *", async () => {
//   console.log("⏰ Running calendar→booking sync job...");
//   try {
//     const updated = await syncConfirmedBookingsFromCalendar();
//     console.log(`✅ Calendar sync complete – updated ${updated} bookings.`);
//   } catch (err) {
//     console.error("❌ Calendar sync cron job failed:", err);
//   }
// });

// ------------------ Start server ------------------

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
