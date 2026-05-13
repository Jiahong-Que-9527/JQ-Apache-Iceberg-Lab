# Contributor Policy

This repository is maintained as a single-author portfolio and learning project.

## Required Git Identity

The only allowed Git contributor is:

```text
Jiahong Que <jiahongque25@gmail.com>
```

All code agents, automation, and local tooling must preserve this exact Git identity for every commit.

## Rules for Code Agents

Code agents must follow these rules before committing or pushing:

- Confirm `git config user.name` is `Jiahong Que`.
- Confirm `git config user.email` is `jiahongque25@gmail.com`.
- Confirm `git shortlog -sne --all` contains no contributor other than `Jiahong Que <jiahongque25@gmail.com>`.
- Do not add `Co-authored-by` trailers.
- Do not add bot names, tool names, or generated-by identities to commits.
- Do not merge commits authored by other people into this repository.

If any command shows another contributor, stop and ask Jiahong before continuing.

## External Feedback

External feedback is welcome through issues, conversations, or suggested text, but repository commits must remain authored only by Jiahong Que.

