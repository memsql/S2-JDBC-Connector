# AGENTS.md

## Cursor Cloud specific instructions

This repo is the **SingleStore JDBC Driver** — a Java 11 / Maven *library* (JDBC 4.2 driver), not a standalone app. "Running" it means connecting to a SingleStore server and executing SQL through the driver. Standard commands live in `README.md` (build) and `CONTRIBUTING.md` (test); this section only captures non-obvious cloud caveats.

### Java / Maven
- Build and tests target **JDK 11** (matches CI in `.circleci/config.yml` / `.github/workflows/test.yml`). JDK 11 is the default `java`/`javac` (set via `update-alternatives`), so `mvn` uses it automatically — do not override `JAVA_HOME`.
- Build the driver jar: `mvn -Dmaven.test.skip -Dmaven.javadoc.skip package` (output in `target/`).
- There is no separate lint gate; code formatting is enforced by the `git-code-format-maven-plugin` (google-java-format) git hooks, not a Maven verify goal.

### Tests
- Unit tests need **no database**: `mvn test -Dtest='com.singlestore.jdbc.unit.**' -DfailIfNoTests=false`.
- Integration tests (everything under `com.singlestore.jdbc.integration`, plus a few `unit` tests that extend `integration.Common`) require a running SingleStore server. Connection defaults are in `src/test/resources/conf.properties` (`localhost:5506`, db `test`, user `root`, password `password`) and are overridable via `TEST_DB_HOST` / `TEST_DB_PORT` / `TEST_DB_USER` / `TEST_DB_PASSWORD` env vars or system properties.
- Run the whole suite with `mvn test`, or a single class with `-Dtest=com.singlestore.jdbc.integration.StatementTest`.
- `Test_SingleStore.java` at the repo root is a legacy smoke test that **fails against SingleStore 9.x** (it creates users via deprecated `GRANT`). Use `mvn test` instead.

### Starting the SingleStore database (needed for integration tests)
The DB runs in Docker and is **not** a persistent service — after a VM restart you must restart the Docker daemon and re-create the container.
1. Start the daemon (once per VM boot): `sudo dockerd > /tmp/dockerd.log 2>&1 &` (a tmux session named `dockerd` is the convenient way). The daemon is configured for `fuse-overlayfs`.
2. Bring up the cluster: `ROOT_PASSWORD=password SINGLESTORE_LICENSE="$SINGLESTORE_LICENSE" sudo -E ./scripts/ensure-test-singlestore-cluster-password.sh`.
   - `SINGLESTORE_LICENSE` must be **set** (the script runs under `set -u`), but it can be empty — the dev image no longer requires a license to start.

**With a valid `SINGLESTORE_LICENSE`** (already configured as a project secret, so it is injected into the environment and picked up automatically by the command above): the script runs to completion — it adds the child aggregator (`ADD AGGREGATOR` succeeds), creates the `test` database, and sets up SSL / JWT / root-ssl fixtures, ending with `Done!`. No manual follow-up is needed, and the full suite (including `SslTest`) can run.

**Without a license (empty `SINGLESTORE_LICENSE` → free Developer Image Edition):** the script **fails at the `ADD AGGREGATOR` step** with `Feature 'child aggregators' is not supported in SingleStore Developer Image Edition`, then exits early (`set -e`). This is expected in the free edition, which allows only the master aggregator on port 5506 — enough for most integration tests. The container `singlestore-integration` stays up and healthy, but you must finish the skipped steps manually:
- Create the test database: `mysql -u root -h 127.0.0.1 -P 5506 -ppassword -e 'CREATE DATABASE IF NOT EXISTS test'`.
- SSL / JWT / PAM / Kerberos fixtures are not set up, so those tests (e.g. `SslTest`) will fail unless you complete that setup manually.

Verify either way: `mysql -u root -h 127.0.0.1 -P 5506 -ppassword -e 'SELECT @@memsql_version'`.
