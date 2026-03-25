Create a new release for version $ARGUMENTS.

Follow these steps:

1. **Determine changelog entries**: Run `git log` to find all commits since the last release tag. Summarize meaningful changes (features, bug fixes, dependency updates) for the changelog. Skip test-only changes (commits prefixed with `test:`).

2. **Update pom.xml**: Change the `<version>` field and the `<tag>` field in the `<scm>` section to the new version. The tag format is `singlestore-jdbc-client-<version>`.

3. **Update CHANGELOG.md**: Add a new section at the top (below the `# SingleStore Change Log` header) with the format:
   ```
   ## [<version>](https://github.com/memsql/S2-JDBC-Connector/releases/tag/v<version>)
   * <change description> (#PR)
   ```

4. **Create a branch**: `release-<version>`

5. **Show the diff** to the user for review before committing.

6. **After user confirms**: Commit with message `chore: bump version to <version>` and body `Incremented version, updated changelog`.

7. **Push and open PR** with title `chore: bump version to <version>` and body `Incremented version, updated changelog`.
