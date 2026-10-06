# Pilot survey: your Mythic+ history

A short pilot (target: 20–50 responses from people you know: guildmates, Discord, past
groupmates) to test the questions before building a Battle.net-verified version.

- **Build it:** open [script.google.com](https://script.google.com), create a project, paste
  [`create_form.gs`](create_form.gs), run `createSurvey`, approve the permissions. It logs the
  form's edit link and public link.
- **Contact for deletion requests:** epicparse.research@gmail.com (set in both files).
- **Characters are self-reported** in the pilot and checked against Raider.IO; ownership isn't
  verified until the Battle.net version.
- Estimated time to complete: 8–12 minutes.

## Consent text (shown first)

> This survey is part of Epic-Parse, a personal research project on how different kinds of
> World of Warcraft players experience Mythic+: what they aim for, how they find groups, and
> why they stop pushing. Results may appear in blog posts.
>
> - Taking part is voluntary, and every question except this one is optional.
> - Results are only published in aggregate. Nothing is published with your character names.
> - Your written answers are only quoted (anonymously) if you say yes at the end.
> - If you list characters, their public Mythic+ and raid data may be looked up on
>   Raider.IO and linked to your answers, only if you allow it.
> - You can ask for your answers to be deleted at any time: epicparse.research@gmail.com.
>   Saving your response's edit link also lets you change it later.
> - No email address is collected.

## Questions and codebook

Seasons offered: TWW S1, TWW S2, TWW S3, Midnight S1, Midnight S2 "so far" while the season is running (earlier seasons go in a
free-text question to keep the pilot short).

| # | Question | Type | Maps to (`gold_labels.trait` unless noted) |
|---|---|---|---|
| 0 | I've read the above and agree to take part | Required choice | (consent; responses without it are discarded) |
| 1 | Which region do you mostly play in? | Choice: US / EU / Oceanic / KR / TW | `players.notes` (project is US-focused) |
| 2a | When did you start doing Mythic+ regularly? | Dropdown: Legion or earlier, BfA S1 … Midnight S2, I don't do M+ regularly | `mplus_start` |
| 2b | Have you ever made a real effort to push your rating? | Choice: yes, most seasons / yes, in some seasons / not really — I run keys for gear, fun or friends / no | `pushing_history` |
| 2c | If so, when did you first push? | Dropdown (same seasons), optional | `climbing_interest` = started (that season) |
| 3 | What do you mostly play? | Checkboxes: M+, raid, rated PvP, delves/solo, RP/collecting/transmog, other | `content_*` booleans |
| 4 | Which one matters most to you? | Choice (same list) | `content_primary` |
| 5 | What role(s) do you play in M+ nowadays? | Checkboxes: tank, healer, DPS | `role` (general) |
| 6 | Why do you play M+? | Scale 1–5: "for fun / the experience" → "to climb / results" | `focus` (1–2 experience, 4–5 outcome, 3 mixed) |
| 7 | How do you usually form groups? | Choice: mostly pug, static, guild, friends, mix | `social_mode` (general) |
| 8 | Your characters (Name-Realm, one per line; put * after this season's main) | Paragraph | `characters` + `player_characters` (how = `self_reported`) |
| 9 | May we look up these characters on Raider.IO and link them to your answers? | Choice: yes / no | consent flag for lookups |
| 10 | For each season: how did you group? | Grid: rows = seasons; columns = didn't play M+, mostly pug, static/premade, static then pug, mix | `social_mode` (per season) |
| 11 | For each season: how did it go for you? | Grid: rows = seasons; columns = didn't play M+, played casually (wasn't pushing), pushed to the end, pushed then stopped partway, pushed then stopped early, pushed then stopped very early | `persistence` (per season; casual = not pushing, not "stopped") |
| 12 | If you stopped: what happened? (tick all, any order) | Checkbox grid: rows = seasons; columns = group fell apart, pugging got too frustrating, burned out, lost interest, life got busy, hit my goal, gear/loot luck, other | `stop_reason` (multiple per season = the chain) |
| 13 | In your own words: what happened in a season where you stopped, or kept going? | Paragraph | free text: classifier training data |
| 14 | Anything from earlier seasons (Legion, BfA, Shadowlands, Dragonflight) worth knowing? | Paragraph | free text; era notes |
| 15 | How do you feel about loot randomness? | Choice: love the thrill of drops / prefer guaranteed (vault, crests, catch-up currencies) / like a mix / don't care | `loot_attitude` |
| 16 | How has M+ changed for you over the years? | Paragraph | free text; `season_rules.notes` ideas |
| 17 | May we quote your written answers anonymously on the blog? | Choice: yes / no | quote permission |
| 18 | Optional: Discord or BattleTag if you're OK with a follow-up | Short text | stored separately from answers; never published |

### Notes on the design

- **Stop reasons are a chain, not a single cause** (group dissolves → back to pugging → burnout
  → interest fades → stop), so question 12 allows several per season. The free text in 13 is
  where the order shows.
- **Don't assume everyone pushes.** Casual players are the comparison group, so questions are
  worded to fit them too (2a–2c are split for that reason, and question 11 has a "played
  casually" option, so not pushing is never recorded as "stopped").
- **No score questions.** Scores come from Raider.IO, which is more reliable than memory.
- **Every question except consent is optional**, so a partial history is still useful.
- Answers are `labeled_by = 'respondent (self-reported)'`, the same standard as the owner's gold
  labels.

## After the pilot

- Look at skipped and confusing questions, and how long people took.
- Import: export responses as CSV; a small importer turns them into `players`,
  `player_characters` and `gold_labels` (to be written once the questions settle).
- Then build the Battle.net-login version (verified characters and alts).
