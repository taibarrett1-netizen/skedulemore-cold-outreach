-- Atomic scrape job claim (SKIP LOCKED) + lease heartbeat. Service role only.

CREATE OR REPLACE FUNCTION public.claim_cold_dm_scrape_job(p_worker_id text, p_lease_seconds integer)
RETURNS SETOF public.cold_dm_scrape_jobs
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  lease interval;
BEGIN
  lease := make_interval(secs => GREATEST(30, LEAST(COALESCE(NULLIF(p_lease_seconds, 0), 180), 3600)));
  RETURN QUERY
  WITH picked AS (
    SELECT id
    FROM public.cold_dm_scrape_jobs
    WHERE status = 'pending'
    ORDER BY started_at ASC NULLS LAST
    FOR UPDATE SKIP LOCKED
    LIMIT 1
  )
  UPDATE public.cold_dm_scrape_jobs j
  SET
    status = 'running',
    leased_by_worker = p_worker_id,
    leased_until = NOW() + lease,
    lease_heartbeat_at = NOW(),
    attempt_count = j.attempt_count + 1
  FROM picked
  WHERE j.id = picked.id
  RETURNING j.*;
END;
$$;

REVOKE ALL ON FUNCTION public.claim_cold_dm_scrape_job(text, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.claim_cold_dm_scrape_job(text, integer) TO service_role;

CREATE OR REPLACE FUNCTION public.heartbeat_cold_dm_scrape_job(p_job_id uuid, p_worker_id text, p_lease_seconds integer)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  lease interval;
  updated int;
BEGIN
  lease := make_interval(secs => GREATEST(30, LEAST(COALESCE(NULLIF(p_lease_seconds, 0), 180), 3600)));
  UPDATE public.cold_dm_scrape_jobs
  SET
    leased_until = NOW() + lease,
    lease_heartbeat_at = NOW()
  WHERE id = p_job_id
    AND leased_by_worker = p_worker_id
    AND status = 'running';
  GET DIAGNOSTICS updated = ROW_COUNT;
  RETURN updated > 0;
END;
$$;

REVOKE ALL ON FUNCTION public.heartbeat_cold_dm_scrape_job(uuid, text, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.heartbeat_cold_dm_scrape_job(uuid, text, integer) TO service_role;
