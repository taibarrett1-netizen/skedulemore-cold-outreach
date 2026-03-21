# Follow-up sends (`POST /api/follow-up/send`)

SkeduleMore **follow-ups** are triggered by the dashboard when a scheduled follow-up runs. The VPS only receives an HTTP request; it does **not** read follow-up configuration from `cold_dm_campaigns`, `cold_dm_message_group_messages`, or any follow-up-specific DB tables.

## Voice notes (follow-ups)

- **Storage:** Dashboard stores audio in Supabase Storage (e.g. bucket `voice-notes`). No extra cold-DM tables are required for follow-up voice.
- **Config:** Follow-up definitions live in `bot_config.follow_ups[]` (including `audio_url` or equivalent on the dashboard side). The VPS does not query `bot_config`.
- **Request:** When sending, the dashboard calls `POST /api/follow-up/send` with:
  - `audioUrl`: HTTPS URL (typically a **time-limited signed URL**) for the audio file
  - `caption` (optional): sent as a text DM before the voice note, in the same thread
- **VPS behavior:** Downloads the file from `audioUrl`, plays it into the virtual Pulse sink, and drives Instagram Web’s mic UI (same helpers as cold-DM voice notes). Session comes from `clientId` + `instagramSessionId` (`cold_dm_instagram_sessions` row).

## Cold DM campaign voice (separate)

Voice for **cold outreach campaigns** may use `cold_dm_campaigns` / `cold_dm_message_group_messages` columns (see migration `010_voice_notes.sql`) and env `VOICE_NOTE_*` on the worker. That path is **independent** from follow-up sends.
