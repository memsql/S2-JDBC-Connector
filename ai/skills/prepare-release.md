Create a new release for version $ARGUMENTS.

Follow these steps:

1. **Determine changelog entries**: Run `git log` to find all commits since the last release tag. Summarize meaningful changes (features, bug fixes, dependency updates) for the changelog. Skip test-only changes (commits prefixed with `test:`).

2. **Update pom.xml**: Change the `<version>` field and the `<tag>` field in the `<scm>` section to the new version. The tag format is `singlestore-jdbc-client-<version>`.

3. **Update CHANGELOG.md**: Add a new section at the top (below the `# SingleStore Change Log` header) with the format:
   ```
   ## [<version>](https://github.com/memsql/S2-JDBC-Connector/releases/tag/v<version>)
   * <change description> (#PR)
   ```

4. **Update .circleci/config.yml**: Change the `path` field in the `store_artifacts` steps of the `test` job to the `target/singlestore-jdbc-client-<version>.jar` and `target/singlestore-jdbc-client-<version>-browser-sso-uber.jar`.

5. **Update README.md**: Update the version number in the `## Version` section and in the `## Obtaining the driver` section.

6**Create a branch**: `release-<version>`

7**Show the diff** to the user for review before committing.

8**After user confirms**: Commit with message `chore: bump version to <version>` and body `Incremented version, updated changelog`.

9**Push and open PR** with title `chore: bump version to <version>` and body `Incremented version, updated changelog`.
