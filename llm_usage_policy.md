# LLM Usage policy

- **`src/` (incl. `examples/`, `Cargo.toml`, `Cargo.lock`):** Don't use LLMs to directly edit. Prefer hand written code and define interfaces by default. It's ok to refer to LLMs for code reviews. Maintain familiarity with low level logic. Only exceptions under src/ are CRUD handlers, clap CLI definitions, reqwest plumbing.
- **Outside `src/` (scripts/smoke tests, milestones docs):** you may use LLMs to implement, but evaluate the change against the high-level directive first, Read through the code and ensure its not breaking existing functionality. Maintain familiarity with high level logic.
- **Always allowed:** Agentic use is allowed for building, testing, running smoke suites, reading/explaining code, and updating planning docs under `milestones/`.

