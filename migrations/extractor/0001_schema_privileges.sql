-- Only the owning migration login may change this schema.
REVOKE ALL ON SCHEMA extractor FROM PUBLIC;
GRANT USAGE ON SCHEMA extractor TO extractor_runtime;
REVOKE CREATE ON SCHEMA extractor FROM extractor_runtime;

-- PUBLIC normally receives function EXECUTE and type USAGE globally.
-- Schema-local revokes cannot remove those global defaults.
ALTER DEFAULT PRIVILEGES FOR ROLE extractor_migrator
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE extractor_migrator
    REVOKE USAGE ON TYPES FROM PUBLIC;

-- No blanket runtime grants: future migrations grant only the required
-- privileges on their own application tables/sequences. Migration metadata
-- remains private to the migrator.
