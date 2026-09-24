#!/usr/bin/env bash
# Runs next to the JobManager and keeps the job "crypto-live" running.
# A Flink session cluster without HA forgets its jobs when the JobManager restarts (for example after a reboot).
# Every 30 s this loop checks the running jobs, and submits the SQL again if the job is missing.
# The SQL is k8s/flink-live.sql, mounted from a ConfigMap at /opt/crypto/sql/live.sql.
set -u
JOB=crypto-live
FLINK=/opt/flink/bin

until "$FLINK/flink" list -r >/dev/null 2>&1; do
  echo "waiting for the JobManager"
  sleep 5
done

while true; do
  if ! "$FLINK/flink" list -r 2>/dev/null | grep -qE ": $JOB \((RUNNING|RESTARTING|CREATED|INITIALIZING)\)"; then
    echo "$(date -u +%FT%TZ) job $JOB is not running: submitting the SQL"
    "$FLINK/sql-client.sh" -f /opt/crypto/sql/live.sql || echo "submit failed; trying again in 30 s"
  fi
  sleep 30
done
