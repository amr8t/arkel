# Arkel website

Plain [Zola](https://www.getzola.org/) static site, gh-pages-ready.

## Edit & build locally

```sh
zola --root website serve   # live preview at http://127.0.0.1:1111
zola --root website build   # outputs website/public (gitignored)
```

Pages are Markdown in `website/content/`. Templates in `website/templates/`,
styling in `website/static/style.css`. Nothing to configure — content is the site.

## Deploy

Pushing to `main` when anything under `website/` changes runs
`.github/workflows/deploy.yml`, which builds and publishes `website/public` to
the `gh-pages` branch (enable GitHub Pages → "Deploy from a branch" → `gh-pages`).

## Releases / version bumps

`cargo` version lives in the root `Cargo.toml`. Bump + tag with
[`cargo-edit`](https://github.com/killercup/cargo-edit):

```sh
cargo install cargo-edit
cargo set-version 0.2.0
git commit -am "v0.2.0" && git tag v0.2.0 && git push --tags
```

The tag triggers `.github/workflows/release.yml`: builds the release binary and
attaches it to a GitHub Release. `cargo test` + clippy + fmt run on every push
via `.github/workflows/ci.yml`.
