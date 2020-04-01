# monetDB-backup

A backup service that its purpose is to run from a cron. The cron must run often enough, so that if
the machine closes a backup will end. The script also checks if the existing backups are old enough,
so that a new must be created.
