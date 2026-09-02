# Releasing

A JDBC release is triggered by pushing a version tag to GitHub. The [Release](.github/workflows/release.yml) workflow then publishes artifacts to Maven Central and creates a draft GitHub Release for manual review before publishing.

## Prerequisites

Repository secrets used by the release workflow:

| Secret | Purpose |
| --- | --- |
| `ENCRYPTION_KEY` / `ENCRYPTION_IV` | Decrypt the GPG signing key in `ci/secring.asc.enc` |
| `SONATYPE_USER` / `SONATYPE_PASSWORD` | Authenticate to Maven Central (Sonatype) |
| `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` / `AZURE_SUBSCRIPTION_ID` | Azure OIDC login for Trusted Signing |

Repository variables used for Azure Trusted Signing:

| Variable | Purpose |
| --- | --- |
| `AZURE_SIGNING_ENDPOINT` | Trusted Signing endpoint URL (Jsign `--keystore`) |
| `AZURE_SIGNING_ACCOUNT`  | Trusted Signing account name |
| `AZURE_SIGNING_PROFILE`  | Trusted Signing certificate profile name |

## Using AI release skills

The `ai/skills/` folder has agent skills that automate the manual steps below. Ask an AI coding agent (for example Cursor) to follow the skill and pass the version as the argument:

| Skill | File | What it does |
| --- | --- | --- |
| Prepare release | [`ai/skills/prepare-release.md`](ai/skills/prepare-release.md) | Bumps version files, updates the changelog, opens a `release-<version>` PR |
| Publish release | [`ai/skills/publish-release.md`](ai/skills/publish-release.md) | After the PR is approved and green, merges it and pushes the `v<version>` tag |

Example prompts:

```text
Follow ai/skills/prepare-release.md for version 1.2.13
```

```text
Follow ai/skills/publish-release.md for version 1.2.13
```

Run prepare first, wait for the PR to be reviewed and CI to pass, then run publish. Pushing the tag is what triggers the Release workflow.

## 1. Prepare the release

Use the [prepare-release](ai/skills/prepare-release.md) skill or follow the steps below.

Create a `release-<version>` branch and bump the project version. For example, for `1.2.13`:

1. Update `pom.xml`:
   - Set `<version>` to the new version.
   - Set `<scm><tag>` to `singlestore-jdbc-client-<version>`.
2. Add a section to `CHANGELOG.md` for the new version, summarizing changes since the previous release tag.
3. Update the version in `.circleci/config.yml` `store_artifacts` paths for the JAR files.
4. Update the version in `README.md` (`## Version` and the Maven dependency example).
5. Open a PR, wait for CI and review, then merge.


## 2. Publish by pushing a tag

After the version bump is on `master`, use the [publish-release](ai/skills/publish-release.md) skill, which merges the release PR (when checks and approval are in place) and pushes the tag, or do this manually:

```bash
git checkout master
git pull
git tag -a v<version> -m "Release <version>"
git push origin v<version>
```

Tag format: `v` followed by the exact `pom.xml` version (for example `v1.2.13` or `v1.2.13-beta`).

Pushing the tag starts the Release workflow, which:

1. Builds and deploys GPG-signed artifacts to Maven Central.
2. Signs the release JARs with Azure Trusted Signing via `jarsigner` and Jsign's JCA provider.
3. Imports the Microsoft Identity Verification Root Certificate Authority 2020 into the JDK truststore. Trusted Signing chains to this root, and the JDK does not ship it, so `jarsigner` cannot validate the signer or timestamp chains without it. Anyone verifying the published JARs on a stock JDK will see `PKIX path building failed` warnings unless they import the same root.
4. Verifies the JAR signatures with `jarsigner -verify`.
5. Creates a **draft** GitHub Release named `SingleStore JDBC Driver <version>` with generated release notes.
6. Attaches the signed JARs:
   - `singlestore-jdbc-client-<version>.jar`
   - `singlestore-jdbc-client-<version>-browser-sso-uber.jar`

## 3. Publish the GitHub Release

After the workflow succeeds:

1. Open the draft [GitHub Release](https://github.com/memsql/S2-JDBC-Connector/releases) for the new tag.
2. Review and edit the auto-generated release notes (align with `CHANGELOG.md` as needed).
3. Confirm the expected JARs are attached.
4. Publish the release and mark it as the latest release.

## 4. Verify

1. Confirm the [Release](https://github.com/memsql/S2-JDBC-Connector/actions/workflows/release.yml) workflow succeeded.
2. Confirm the published [GitHub Release](https://github.com/memsql/S2-JDBC-Connector/releases) is marked as latest with the expected JARs and finalized notes.
3. Confirm the artifact appears on [Maven Central](https://central.sonatype.com/artifact/com.singlestore/singlestore-jdbc-client) (propagation can take some time).

## Driver-Server Version Compatibility Matrix

After each release, add a row for the new version rather than copying an older row's engine list. CircleCI pinned engine images (and later explicit `singlestore_version` values) through `v1.2.7`. From `v1.2.8`, GitHub Actions is unpinned (`singlestore-labs/singlestore-supported-versions` at workflow time plus `singlestoredb-dev:latest`); take those lists from the [EOL policy](https://docs.singlestore.com/db/v9.1/support/singlestore-software-end-of-life-eol-policy/) as of the new tag's date, plus any engine RC that existed by that date.

| Driver Version | Release date | Supported engine versions |
| -------------- | ------------ | ------------------------- |
| 1.2.12         | 2026-08-07   | 8.9, 9.0, 9.1 RC          |
| 1.2.11         | 2026-04-14   | 8.7, 8.9, 9.0, 9.1 RC     |
| 1.2.10         | 2026-03-25   | 8.7, 8.9, 9.0, 9.1 RC     |
| 1.2.9          | 2025-11-12   | 8.5, 8.7, 8.9, 9.0        |
| 1.2.8          | 2025-05-09   | 8.5, 8.7, 8.9             |
| 1.2.7          | 2025-01-08   | 8.1, 8.5, 8.7, 8.9        |
| 1.2.6          | 2024-11-07   | 8.0, 8.1, 8.5, 8.7        |
| 1.2.5          | 2024-09-23   | 8.0, 8.1, 8.5, 8.7        |
| 1.2.4          | 2024-09-06   | 8.0, 8.1, 8.5             |
| 1.2.3          | 2024-05-17   | 7.8, 8.0, 8.1, 8.5        |
| 1.2.2          | 2024-03-06   | 7.8, 8.0, 8.1, 8.5        |
| 1.2.1          | 2024-01-12   | 7.8, 8.0, 8.1             |
| 1.2.0          | 2023-10-16   | 7.5, 7.6, 7.8, 8.0, 8.1   |
| 1.1.9          | 2023-08-03   | 7.5, 7.6, 7.8, 8.0, 8.1   |
| 1.1.8          | 2023-07-17   | 7.5, 7.6, 7.8, 8.0, 8.1   |
| 1.1.7          | 2023-07-10   | 7.5, 7.6, 7.8, 8.0        |
| 1.1.6          | 2023-07-04   | 7.5, 7.6, 7.8, 8.0        |
| 1.1.5          | 2023-03-29   | 7.5, 7.6, 7.8, 8.0        |
| 1.1.4          | 2022-07-26   | 7.1, 7.3, 7.5, 7.6, 7.8   |
| 1.1.3          | 2022-07-13   | 7.1, 7.3, 7.5, 7.6, 7.8   |
| 1.1.2          | 2022-07-06   | 7.1, 7.3, 7.5, 7.6, 7.8   |
| 1.1.1          | 2022-06-13   | 7.1, 7.3, 7.5, 7.6, 7.8   |
| 1.1.0          | 2022-06-02   | 7.1, 7.3, 7.5, 7.6        |
| 1.0.2          | 2022-05-04   | 7.1, 7.3, 7.5, 7.6        |
| 1.0.1          | 2021-12-17   | 7.1, 7.3, 7.5, 7.6        |
| 1.0.0          | 2021-12-06   | 7.1, 7.3, 7.5, 7.6        |
