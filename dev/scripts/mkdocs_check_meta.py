"""mkdocs hook: fail `--strict` builds on missing or unparsed front matter.

A `description:` value with an unquoted `: ` is invalid YAML; mkdocs swallows
the parse error, renders the block into the page body as a heading, and drops
the real meta tag. Every manual page must carry `description:` (AGENTS.md), so
an empty page.meta here means the front matter is missing or didn't parse.
"""

import logging

log = logging.getLogger("mkdocs.hooks.check_meta")


def on_page_markdown(markdown, page, config, files):
    description = page.meta.get("description")
    if not description:
        log.warning("%s: missing or unparsed front-matter `description:`", page.file.src_uri)
    elif len(description) > 160:
        log.warning(
            "%s: description is %d chars (limit 160)",
            page.file.src_uri,
            len(description),
        )
    return markdown
