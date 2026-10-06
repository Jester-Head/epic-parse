# Pilot survey: your Mythic+ experience

A short pilot (target: 20–50 responses from people you know: guildmates, Discord, past
groupmates) to test the questions before building a Battle.net-verified version.

- **Build it:** open [script.google.com](https://script.google.com), paste
  [`create_form.gs`](create_form.gs) into an empty project, pick a function in the dropdown next
  to **Run**: `createSurvey` for a new form, or `rebuildSurvey` to replace the questions of the
  existing pilot form (same links; refuses if it already has responses).
- **After building:** turn on **Shuffle option order** for question 9.
- **Contact for deletion requests:** epicparse.research@gmail.com (set in both files).
- **Characters are self-reported** in the pilot and checked against Raider.IO; ownership isn't
  verified until the Battle.net version.
- Estimated time to complete: about 10 minutes.

## Design rules (read before editing)

**Neutrality.** The first draft was built from one player's own history (pugging, burnout, a
group falling apart) and primed respondents to repeat it. So:

- No examples or wording taken from any one person's story, including the project owner's.
- Don't presuppose a play style: nobody should feel the survey assumes they push, pug, burn out
  or stop. Casual players are the comparison group.
- Broad, balanced answer lists with "Other (please specify)", "Not applicable" and
  "Don't remember" where they fit. Shuffle reason lists.
- Rate motivations separately (an importance grid), never as a forced trade-off.
- Match the answer format to the question: a "why" question needs reasons, not a number.
- Ask how much people played per patch, never "when did you stop": stopping, returning and
  starting late show up in the sequence.

**Standard format.**

- Order: consent → main topic (general → specific) → background questions last.
- Screener with branching: people who never run Mythic+ skip the Mythic+ sections.
- Standard 5-point scales with labeled points: frequency (Never → Very often), importance (Not at
  all → Extremely important, plus Not applicable), agreement (Strongly disagree → Strongly agree).
- Behavioral anchors instead of vague quantifiers: keys per week (None, 1–3, 4–7, 8–15, 16+),
  which also match the Great Vault thresholds players think in.
- "Select all that apply" followed by "main reason" for causes.
- Activities are listed separately and grouped at analysis time (e.g. housing is its own row
  rather than being declared "solo" or "social" in the question).

## Consent text (shown first)

> - Taking part is voluntary, and every question except this one is optional.
> - There are no right or wrong answers. Every play style is welcome.
> - Results are only published in aggregate. Nothing is published with your character names.
> - Written answers are only quoted (anonymously) if you give permission at the end.
> - If you list characters, their public Mythic+ and raid data may be looked up on
>   Raider.IO and linked to your answers, only if you allow it.
> - You can ask for your answers to be deleted at any time: epicparse.research@gmail.com.
>   Saving your response's edit link also lets you change it later.
> - No email address is collected.

## Structure and codebook

| Section | # | Question | Format | Maps to (`gold_labels.trait` unless noted) |
|---|---|---|---|---|
| Consent | 0 | I have read the information above and agree to take part | Required choice | consent; responses without it are discarded |
| How you play | 1 | How often do you do each of these activities? | Grid: rows = Mythic+, raiding, rated PvP, delves, questing/world content, housing, collecting, professions/AH, role-play, alts; columns = Never → Very often | `activity_frequency_*` (one per row); `content_*` derived |
| | 2 | Which activity matters most to you? | Choice + Other | `content_primary` |
| | 3 | In the past two years, how often have you run Mythic+ dungeons? | Choice (Never → Very often); **Never skips to "About you"** | `mplus_frequency`; screener |
| Mythic+ | 4 | Which role(s) do you play in Mythic+? | Checkboxes: tank, healer, damage | `role` (general) |
| | 5 | How important is each of the following to you when you play Mythic+? | Grid: rows = gear, rewards, rating, improving, friends/guild, enjoying dungeons, competing, short sessions; columns = 5-point importance + Not applicable | `motivation_*`; `focus` derived, never asked |
| | 6 | How often do you set a rating or achievement goal for a season? | Choice: never / some / most / every season | `goal_setting` |
| | 7 | How do you usually find Mythic+ groups? | Choice + Other | `social_mode` (general) |
| Season by season | 8 | For each patch, roughly how many keys did you run per week? | One grid per season; rows = patch periods (x.0 start, x.5, x.7, plus the Midnight pre-patch in TWW S3) with month ranges; columns = None, 1–3, 4–7, 8–15, 16+, Don't remember | `keys_per_week` per (season, patch); `persistence`, stop patch and returns are **derived** |
| Changes during a season | 9 | Times you ran fewer keys later in a season, or stopped: which contributed? | Checkboxes (shuffled) + Not applicable + Other | `stop_reason` (several possible) |
| | 10 | Which was the main reason? | Choice + Not applicable + Other | `stop_reason_main` |
| Your views | 11 | Agreement with: enjoy random loot; prefer choosable rewards; more fun with people I know; asks for more time than I want to give; rewards worth the effort | Grid: 5-point agreement | `loot_attitude` (first two); `attitude_*` |
| In your own words | 12 | A season that stands out, good or bad; what made it that way | Paragraph | free text: classifier training data |
| | 13 | How has Mythic+ changed for you over the years? | Paragraph | free text; `season_rules.notes` ideas |
| | 14 | Anything from earlier seasons worth knowing? | Paragraph | free text; era notes |
| About you | 15 | Region | Choice | `players.notes` (project is US-focused) |
| | 16 | When did you start running Mythic+ regularly? | Dropdown (seasons) + "have not" | `mplus_start` |
| | 17 | Characters (Name-Realm, one per line; * = current main) | Paragraph, optional | `characters` + `player_characters` (how = `self_reported`) |
| | 18 | May we look up these characters on Raider.IO? | Choice | consent flag for lookups |
| Before you finish | 19 | May we quote your written answers anonymously? | Choice | quote permission |
| | 20 | Optional: Discord or BattleTag for a follow-up | Short text | stored separately; never published |

Answers are stored with `labeled_by = 'respondent (self-reported)'`.

## After the pilot

- Look at skipped and confusing questions, completion time, and where people dropped out.
- Check for signs of priming (many respondents echoing the same phrasing).
- Import: export responses as CSV; a small importer turns them into `players`,
  `player_characters` and `gold_labels` (to be written once the questions settle).
- When patch 12.1.5 is released, add it to the Midnight S2 rows (before responses exist, or in
  a new form).
- Then build the Battle.net-login version (verified characters and alts).
