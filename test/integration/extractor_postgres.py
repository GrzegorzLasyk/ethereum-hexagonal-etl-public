#!/usr/bin/env python3
"""Stage 5 tests on a disposable Compose project, never an existing database."""

import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import uuid


def main():
    root = Path(__file__).resolve().parents[2]
    project = "ethereum-stage5-" + uuid.uuid4().hex[:12]
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    names = ["POSTGRES_PASSWORD"] + [
        f"{service}_{role}_PASSWORD"
        for service in ("EXTRACTOR", "TRANSFORMER", "LOADER")
        for role in ("MIGRATOR", "RUNTIME")
    ]
    passwords = {name: secrets.token_hex(24) for name in names}
    # mkstemp creates mode 0600. Do not print or put credentials in arguments.
    fd, filename = tempfile.mkstemp(prefix="ethereum-stage5-", suffix=".env")
    with os.fdopen(fd, "w") as config:
        for name, value in passwords.items():
            config.write(f"{name}={value}\n")
        config.write(f"POSTGRES_PORT={port}\n")
    dc = ["docker", "compose", "--env-file", filename, "-p", project,
          "-f", str(root / "deployments/compose/compose.yaml")]
    # Shell environment takes precedence over --env-file: remove inherited
    # Compose credentials/port so this project always uses its own test secrets.
    env = os.environ.copy()
    for name in names + ["POSTGRES_PORT"]:
        env.pop(name, None)

    def run(command, *, environment=env):
        subprocess.run(command, cwd=root, env=environment, check=True)

    try:
        run(dc + ["config", "--quiet"])
        run(dc + ["up", "-d", "--wait", "--wait-timeout", "120", "postgres"])
        # Existing suite applies all migrations, repeats/concurrently reapplies
        # them, checks rollback/checksums, and exercises cross-schema isolation.
        run(dc + ["run", "--rm", "verify"])
        test_env = env.copy()
        for role, variable in [("RUNTIME", "EXTRACTOR_TEST_DATABASE_URL"),
                               ("MIGRATOR", "EXTRACTOR_TEST_MIGRATOR_URL")]:
            test_env[variable] = (
                f"postgres://extractor_{role.lower()}:"
                f"{passwords[f'EXTRACTOR_{role}_PASSWORD']}"
                f"@127.0.0.1:{port}/ethereum_etl?sslmode=disable"
            )
        run(["go", "test", "-tags", "postgresintegration", "-race",
             "-run", "^TestPostgresIntegration$", "-count=1", "-timeout=3m", "-v",
             "./services/extractor-service/internal/adapters/outbound/postgres"],
            environment=test_env)
    finally:
        # These names are freshly generated for this run. No default project,
        # user-configured connection, or pre-existing volume is removed.
        try:
            subprocess.run(dc + ["down", "--volumes", "--remove-orphans"],
                           cwd=root, env=env, check=True, timeout=60)
        finally:
            Path(filename).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
