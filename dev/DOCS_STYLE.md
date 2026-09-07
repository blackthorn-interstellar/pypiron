# Writing the docs

Write for someone choosing or using a PyPI server. Cut aggressively. Keep the
words that make the next sentence or command understandable.

## Landing pages

README.md and the comparison pages should make someone want to try pypiron.
Use **bold, succinct, impressive descriptions**. Lead with speed, security,
reliability, and what the reader can run. Link the evidence.

- Keep strong claims. Keep their scope accurate: installs are not requests,
  a sampled traffic rate is not all of PyPI, and a benchmark is not a guarantee.
- Put measurements and their conditions on the evidence page. The landing
  page needs the headline and a link, not the methodology.
- Prefer concrete benefits to adjectives. A number or a named capability
  earns its place; "seamless" and "enterprise-grade" do not.
- Give the visitor a clear next action: try it, deploy it, or migrate.
- Preserve good lines. A rewrite must be shorter, clearer, or more compelling
  to justify replacing them.

README.md is the source for docs/index.md. Keep its one-command start, real
product screenshot, and compact comparison table. Generate the site version
with `make docs`; do not maintain two versions or turn the landing page into a
long tutorial.

## Concepts

Keep one concepts page. Answer the customer's questions in their order:

1. Can I host my private packages?
2. Can I serve public packages too? Can I cache them or sync them for offline use?
3. Where do I store them? How do I add servers?
4. Who can publish and install?
5. What keeps unsafe packages out?
6. How do I check the server and see what people install?

Explain pypiron's behavior, not the reader's own job. An operator does not
need a lesson on what a bucket is. They do need to know whether it must exist
before starting pypiron and which credentials the server uses.

Do not organize around invented product categories: "three sources," "two
shapes," or "the byte gate." Use the words a customer would use.

## Guides

Each guide completes a task. Start with the prerequisites or first action;
the reader already chose the task when they clicked the link.

- State where each command runs and where each file is saved.
- Define placeholders before use. Do not assume `$ADMIN`, a built wheel, a
  running server, or a configured client exists.
- Show the command that performs the action and how to confirm it worked.
- Keep the main path short. Put alternatives and uncommon cases after it.
- Explain a restriction where it changes the next step. Keep data-loss
  warnings beside the command that can lose data.
- Link to reference for all options. Do not paste a full flag table into a
  guide or send the reader away to discover a required step.

## Reference

Keep exact flag names, environment variables, defaults, formats, and behavior.
Use tables for lookup. Explain technical terms when they affect configuration;
standard terminology is useful here. Put longer procedures in guides and
implementation details in `dev/`.

## Every sentence

- Use familiar words and active verbs. Delete filler, repeated pitches,
  throat-clearing, jokes, and commentary about the page itself.
- Fragments are welcome when the meaning is complete. Shortening must not
  leave a reader guessing what a noun, pronoun, or command refers to.
- Replace idioms with actions: "restart using the remaining bucket," not
  "the DR-day exit"; "cannot serve downloads," not "parks the fleet."
- Explain a term before relying on it. Do not rescue a confusing opener with
  an explanation several paragraphs later.
- Do not invent setup times, adoption, guarantees, or claims of independent
  review. Describe what was measured or tested and link the evidence.
- A complete explanation that takes two sentences is better than an opaque
  slogan. A sentence that explains the obvious can usually be deleted.

## Where information belongs

| Reader needs | Home |
| --- | --- |
| Decide whether to try pypiron | README.md → docs/index.md |
| Understand the features | docs/concepts.md |
| Publish packages and configure clients | docs/guides/publish-install.md |
| Run a cloud deployment | docs/guides/standard-cloud.md |
| Run offline, use several regions, or migrate | The corresponding docs/guides/ page |
| Understand protections and their limits | docs/security.md |
| Assess testing and performance | docs/testing.md and docs/compare/index.md |
| Look up a setting | docs/reference/configuration.md |
| Understand implementation or reproduce research | dev/ |

Summarize when the reader needs context; link for detail. Avoid duplicate
procedures and tables that can drift apart.

## Before finishing

Read the page in order as someone who has only its title and the link they
clicked. Record any point where a required fact has not yet been introduced.
Have a separate reader follow materially changed guides and review landing
pages for clear benefits and a useful next action. Persona briefs in
`dev/personas/` can help; report what was actually reviewed, not a blanket
"passed" verdict from an earlier version.

Run the documented commands against the real binary. Run `make check`,
`make docs`, and the CLI/reference drift check. Check links, generated
README/index parity, navigation, search, and page metadata. New pages belong
in both `nav` and the `llmstxt` sections in mkdocs.yml and need a front-matter
description of at most 160 characters.
