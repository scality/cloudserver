# Contributing rules

Please follow the
[Contributing Guidelines](https://github.com/scality/Guidelines/blob/master/CONTRIBUTING.md).

## Development

To set up your development environment, follow these steps:

```bash
yarn install
yarn dev
```

This will start the developement server, watching for changes and restarting
as needed.

### Formatting

Prettier formatting of the whole repository is checked in CI. Run it before
pushing:

```bash
yarn run prettier --write .
```

When reviewing PRs with indentation-only changes, enable **Hide whitespace** in
the GitHub file diff view to make functional changes easier to spot.
