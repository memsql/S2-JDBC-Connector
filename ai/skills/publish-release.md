Publish the release for version $ARGUMENTS.

Follow these steps:

1. **Check CI status**: Run `gh pr checks` on the release PR to verify all checks are green. If any checks are failing, report the failures and stop.

2. **Check PR approval**: Run `gh pr view` to verify the PR has at least one approving review. If not approved, report and stop.

3. **Merge the PR**: Merge the release PR using `gh pr merge --squash`.

4. **Push the tag**: After merging, checkout master, pull latest, and create + push an annotated tag `v<version>` pointing at the merge commit.
   ```
   git tag -a v<version> -m "Release <version>"
   git push origin v<version>
   ```

5. **Confirm** the tag was pushed successfully.
