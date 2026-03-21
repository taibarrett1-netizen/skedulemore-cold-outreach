-- Voice note support for cold DM campaigns / message-group messages only.
-- SkeduleMore follow-up voice does NOT use these columns: the dashboard stores audio in
-- Storage + bot_config.follow_ups and passes a signed audioUrl to POST /api/follow-up/send.

ALTER TABLE public.cold_dm_campaigns
  ADD COLUMN IF NOT EXISTS send_voice_note boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS voice_note_storage_path text,
  ADD COLUMN IF NOT EXISTS voice_note_mode text NOT NULL DEFAULT 'after_text';

ALTER TABLE public.cold_dm_message_group_messages
  ADD COLUMN IF NOT EXISTS send_voice_note boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS voice_note_storage_path text;

COMMENT ON COLUMN public.cold_dm_campaigns.send_voice_note IS 'When true, send a voice note for campaign leads.';
COMMENT ON COLUMN public.cold_dm_campaigns.voice_note_storage_path IS 'Path/URL to audio file used for voice note.';
COMMENT ON COLUMN public.cold_dm_campaigns.voice_note_mode IS 'after_text or voice_only.';
COMMENT ON COLUMN public.cold_dm_message_group_messages.send_voice_note IS 'When true, selected message row sends a voice note.';
COMMENT ON COLUMN public.cold_dm_message_group_messages.voice_note_storage_path IS 'Path/URL to audio file used for voice note.';
