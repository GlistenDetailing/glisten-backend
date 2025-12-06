const { google } = require("googleapis");
const fetch = require("node-fetch");
const path = require("path");
const fs = require("fs");

async function sendTestPush() {
  // Use the FCM key from glisten-prod-a9bb1
  const serviceAccountPath = path.join(__dirname, "fcm-service-account.json");

  // Debug: show which file & project we are using
  const rawJson = fs.readFileSync(serviceAccountPath, "utf8");
  const sa = JSON.parse(rawJson);
  console.log("Using service account file:", serviceAccountPath);
  console.log("  project_id  :", sa.project_id);
  console.log("  client_email:", sa.client_email);

  const auth = new google.auth.GoogleAuth({
    keyFile: serviceAccountPath,
    scopes: ["https://www.googleapis.com/auth/firebase.messaging"],
  });

  const client = await auth.getClient();
  const accessToken = await client.getAccessToken();

  const deviceToken =
    "ciPsgUyoThqEZrxQRwHnro:APA91bEQZLguZgIb_vt6eGuqyEpvh9qhT0D7AOC-_Qo1TxCAmdVB7UpgSZxeux7RQe0YLmKTOoSlEMLYlhfxdNJI6Y49eNDIbCLSfDVorljW6HUKHWD4LDo";

  const message = {
    message: {
      token: deviceToken,
      notification: {
        title: "Glisten Test Push",
        body: "Your FCM v1 push worked!",
      },
    },
  };

  // ✅ Use the real project_id from the key file: glisten-prod-a9bb1
  const projectId = sa.project_id;

  const res = await fetch(
    `https://fcm.googleapis.com/v1/projects/${projectId}/messages:send`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${accessToken.token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(message),
    }
  );

  const data = await res.json();
  console.log("FCM response:", JSON.stringify(data, null, 2));
}

sendTestPush().catch((err) => {
  console.error("Error running sendTestFcm:", err);
});
