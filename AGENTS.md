## Project Guidelines

### Merging PRs

- Use `dev/merge_pr.py` to merge PRs into the Apache Celeborn repository. Do NOT use GitHub's merge button or `git merge` directly.
- After merging, always specify which branch the PR was merged into (e.g. `branch-0.7`, `main`).
- The `dev/merge_pr.py` script is the canonical merge tool for the Celeborn community.
- After merging, leave a comment on the PR thanking the contributor and noting which branch(es) the PR was merged into, including the merge commit hash(es).

### Release Process

- Release branch pattern: `branch-{MAJOR}.{MINOR}` (e.g. `branch-0.7`)
- RC tag pattern: `v{VERSION}-rc{RC_NUMBER}` (e.g. `v0.7.0-rc0`)
- Source and binary artifacts are signed with GPG and uploaded to `dist/dev/celeborn/` (SVN staging)
- Maven artifacts are published to Apache Nexus staging repository
- Only PMC members can write to `dist/release/celeborn/` (including KEYS file updates)
- Vote emails are sent to `dev@celeborn.apache.org` in plain text format

### Language

- Always respond in Simplified Chinese unless explicitly asked otherwise.
- Keep code, commands, technical terms, and file paths in their original form.
