-- Add display_name to cold_dm_leads for first-name extraction (full display string from scraper).
-- Run in Supabase SQL editor or via migrations.

ALTER TABLE public.cold_dm_leads
  ADD COLUMN IF NOT EXISTS display_name TEXT;
