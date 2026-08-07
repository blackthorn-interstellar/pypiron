# Owner-catch regression corpus

Every docs defect the owner catches that the fleet missed becomes a row here
— the gold benchmark, better than any planted defect because each one is a
proven miss. The standing loop, in order, no exceptions (including direct
owner instructions to fix, unless the owner explicitly waives the loop):

1. **Catch** → add the row (exact text, page, commit where it lived).
2. **Strengthen** → patch the instrument *generally* — never a line-shaped
   rule.
3. **Blind flag** → a FRESH actor (never one that has seen the discussion)
   must flag the line unaided. Round 1 missing is a normal outcome; iterate.
4. **Fix** → only now, citing the flag. Prefer the reviewer's own rewrite to
   fresh authorship — reviewer copy has already survived one reader;
   fix-time prose is written with the whole page in context and is never
   experienced in reading order by anyone, including its author.

Re-run rows 1–6 as blind checks whenever actor models or persona files
change, alongside `answer-key.md`.

| # | Caught | Text (where it lived) | Defect class | Instrument | Validated |
| --- | --- | --- | --- | --- | --- |
| 1 | 08-03 | "One index URL, three package sources" / "Three sources, one index" (concepts.md @ 2d6fe97) | outline-counting + in-house taxonomy | veteran heading/restatement | ✔ blind (full-manual pass, unprompted) |
| 2 | 08-03 | "Packages reach that URL three ways. Use one, or all three at once." (concepts.md @ 2d6fe97) | counting + product's chair | veteran restatement | ✔ blind (same run) |
| 3 | 08-03 | the backups paragraph told S3 users about durability (concepts.md @ 2d6fe97) | teaching the reader their own domain | knowledge-inventory boredom (refugee) | ◐ planted exaggeration caught (D1); subtle original untested |
| 4 | 08-07 | "…from one URL, on one config file and one bucket" (standard-cloud.md @ e7089a6) | count drumbeat + broken preposition | veteran **residue test** | ✔ blind round 3 (round 2 missed → residue test added) |
| 5 | 08-07 | "Two shapes, one rule:" (air-gapped.md @ 13299e8^) | cataphoric tagline — order defect | blank-slate incremental reader | ✔ blind, interleaving audited |
| 6 | 08-07 | "Serve your private packages and a cached PyPI from one URL…" as a deploy-guide opener (standard-cloud.md @ f1dc5c7^) | guide opener re-pitches the sale — genre defect | blank-slate reader, task arrival context | ✔ blind, interleaving audited — "getting the elevator pitch again as the first line makes me twitch … one line of re-pitch I didn't need"; twitch-level vs the owner's visceral reaction, but names the exact mechanism (already bought, arrived to deploy) |
