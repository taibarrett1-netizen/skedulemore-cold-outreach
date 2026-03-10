-- Per-message Outreach Start routing: record which cold_dm_message_group_messages row was sent.
-- Enables each message in a group to use its own script path.

ALTER TABLE public.cold_dm_sent_messages
  ADD COLUMN IF NOT EXISTS message_group_message_id UUID REFERENCES public.cold_dm_message_group_messages(id);

CREATE INDEX IF NOT EXISTS idx_cold_dm_sent_messages_message_group_message_id
  ON public.cold_dm_sent_messages(message_group_message_id)
  WHERE message_group_message_id IS NOT NULL;

COMMENT ON COLUMN public.cold_dm_sent_messages.message_group_message_id IS 'The cold_dm_message_group_messages row that was actually sent. Enables per-message Outreach Start routing.';
