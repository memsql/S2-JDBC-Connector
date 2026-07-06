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
2. Bring up the cluster: `ROOT_PASSWORD=password SINGLESTORE_LICENSE="" sudo -E ./scripts/ensure-test-singlestore-cluster-password.sh`.
   - `SINGLESTORE_LICENSE` must be **set** (empty is fine — the dev image no longer needs a license) because the script runs under `set -u`.
   - The script **fails at the `ADD AGGREGATOR` step** with `Feature 'child aggregators' is not supported in SingleStore Developer Image Edition`. This is expected: the free Developer Edition allows only the master aggregator on port 5506, which is all the tests need. After that failure the container `singlestore-integration` is still up and healthy.
3. Create the test database (the script aborts before doing so): `mysql -u root -h 127.0.0.1 -P 5506 -ppassword -e 'CREATE DATABASE IF NOT EXISTS test'`.
4. Verify: `mysql -u root -h 127.0.0.1 -P 5506 -ppassword -e 'SELECT @@memsql_version'`.

SSL / JWT / PAM / Kerberos test setup performed by the script (after the aggregator step) is skipped due to the early exit; the corresponding tests (e.g. `SslTest`) will not have their fixtures and can be ignored unless you complete that setup manually.
