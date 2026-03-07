# Prompt: Add 2FA popup to Cold Outreach Connect (dashboard)

Give this to your dashboard/frontend (e.g. Lovable or other app) so they can add the 2FA flow to the **Connect Instagram** screen.

---

**Backend is already done.** The server keeps the same browser session when 2FA is required and returns a `pending2FAId`. The second step is a separate endpoint so the user’s code is used on that same session (Instagram does not send a second code).

---

## API contract

**Step 1 – Connect**  
- **Endpoint:** `POST /api/instagram/connect`  
- **Body:** `{ username, password, clientId }` (all required when Supabase is used).

**Responses from step 1:**

1. **Success (no 2FA):** `{ ok: true }`  
   → Show success, clear password from the form. Done.

2. **2FA required:** `{ ok: false, code: "two_factor_required", message: "…", pending2FAId: "<id>" }`  
   → Do **not** show a generic error. Store `pending2FAId` and `clientId` in memory (e.g. in component state). Show a **popup/modal** titled e.g. **“Two-factor code required”** with:
   - Short text: “Enter the 6-digit code from your authenticator app or WhatsApp.” (You can add: “Same login session — no new code will be sent.”)
   - One input: 6-digit security code (numeric, max length 6).
   - Buttons: **Submit** and **Cancel**.

3. **Other errors:** `{ ok: false, error: "…" }` or HTTP 4xx/5xx  
   → Show `error` on the Connect form; no 2FA modal.

**Step 2 – Submit 2FA code**  
- **Endpoint:** `POST /api/instagram/connect/2fa`  
- **Body:** `{ pending2FAId, twoFactorCode, clientId }` (all required).  
  Use the `pending2FAId` from step 1 and the `clientId` you stored. `twoFactorCode` = value from the modal input (digits only, first 6 chars).

**Responses from step 2:**

1. **Success:** `{ ok: true }`  
   → Close the modal, show success, clear password and code from the form. Do not store password or code.

2. **Error (wrong/expired code or expired session):** `{ ok: false, error: "…" }`  
   → Show the error in the modal or on the form; e.g. “Code may be wrong or expired. Try again.” or “Session expired. Start Connect again and enter the new code when the popup appears.” Leave the modal open so the user can try another code or cancel.

**Cancel** in the modal closes it and discards the pending step; the user can click Connect again from scratch (they will get a new code from Instagram on the next attempt).

---

## UX summary

- **Connect** → if response is `two_factor_required`, store `pending2FAId` and `clientId`, show the popup. Do **not** call the connect API again with the same username/password and a code; that would start a new login and trigger a second code.
- User enters the **same** code they already received → **Submit** → call `POST /api/instagram/connect/2fa` with `pending2FAId`, `twoFactorCode`, `clientId`. On success, close modal and clear form.
- After any successful connect, never store password or 2FA code; only clear fields and close the modal.
