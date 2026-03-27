-- Repair migration for instances where migration 010 was not applied.
-- Safe to run multiple times.

ALTER TABLE public.cold_dm_message_group_messages
  ADD COLUMN IF NOT EXISTS send_voice_note boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS voice_note_storage_path text;

COMMENT ON COLUMN public.cold_dm_message_group_messages.send_voice_note IS
  'When true, selected message row sends a voice note.';
COMMENT ON COLUMN public.cold_dm_message_group_messages.voice_note_storage_path IS
  'Path/URL to audio file used for voice note.';
