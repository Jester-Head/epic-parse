# Pilot survey: your Mythic+ experience

A short pilot (target: 20–50 responses from people you know: guildmates, Discord, past
groupmates) to test the questions before building a Battle.net-verified version.

- **Build it:** open [script.google.com](https://script.google.com), paste
  [`create_form.gs`](create_form.gs) into an empty project, pick a function in the dropdown next
  to **Run**: `createSurvey` for a new form, or `rebuildSurvey` to replace the questions of the
  existing pilot form (same links; refuses if it already has responses).
- **Contact for deletion requests:** epicparse.research@gmail.com (set in both files).
- **Characters are self-reported** in the pilot and checked against Raider.IO; ownership isn't
  verified until the Battle.net version.
- Estimated time to complete: 8–10 minutes.

## Writing neutral questions (read before editing)

The first draft was built from one player's own history and asked about that path: pugging,
burnout, a group falling apart. That primes respondents to repeat it back. Rules for this survey:

- **No examples or wording taken from any one person's story**, including the project owner's.
- **Don't presuppose a play style.** Casual players are the comparison group; nobody should
  feel the survey assumes they push, stop, burn out or pug.
- **Balanced, broad answer lists** with "Other". No reason gets a privileged position or a
  leading example.
- **Rate motivations separately** (an importance grid), never as a forced trade-off: people can
  care about fun *and* results.
- **Match the answer format to the question.** A "why" question needs reasons, not a number.
- **Ask about change, not about stopping**: "how did your play change over the season" lets
  "stopped" be one answer among several.
- In Google Forms, turn on **Shuffle option order** for the reasons list (question 13) so the
  first option isn't favored.

## Consent text (shown first)

> - Taking part is voluntary, and every question except this one is optional.
> - There are no right answers. Every play style is welcome, including people who rarely run keys.
> - Results are only published in aggregate. Nothing is published with your character names.
> - Your written answers are only quoted (anonymously) if you say yes at the end.
> - If you list characters, their public Mythic+ and raid data may be looked up on
>   Raider.IO and linked to your answers, only if you allow it.
> - You can ask for your answers to be deleted at any time: epicparse.research@gmail.com.
>   Saving your response's edit link also lets you change it later.
> - No email address is collected.

## Questions and codebook

Seasons offered: TWW S1, TWW S2, TWW S3, Midnight S1, Midnight S2 "so far" while the season is
running (earlier seasons go in a free-text question to keep the pilot short).

| # | Question | Type | Maps to (`gold_labels.trait` unless noted) |
|---|---|---|---|
| 0 | I've read the above and agree to take part | Required choice | (consent; responses without it are discarded) |
| 1 | Which region do you mostly play in? | Choice: US / EU / Oceanic / KR / TW | `players.notes` (project is US-focused) |
| 2 | When did you start running Mythic+ regularly? | Dropdown: Legion or earlier, BfA S1 … Midnight S2, I rarely or never run M+ | `mplus_start` |
| 3 | What do you mostly play? | Checkboxes: M+, raid, rated PvP, delves/solo, RP/collecting/transmog, other | `content_*` booleans |
| 4 | Which one matters most to you? | Choice (same list) | `content_primary` |
| 5 | What role(s) do you play in Mythic+? | Checkboxes: tank, healer, DPS | `role` (general) |
| 6 | How important are these to you when you play Mythic+? | Grid: rows = getting gear, earning rewards, raising my rating, improving my own play, playing with friends or guild, enjoying the dungeons, competing with others, short sessions; columns = not important / a little / important / very important | `motivation_*` (one per row); `focus` is derived (rating, improving, competing vs. dungeons, friends, short sessions) rather than asked |
| 7 | Do you set rating or achievement goals in Mythic+? | Choice: never / in some seasons / in most seasons | `goal_setting` |
| 8 | If you do, since about when? | Dropdown (seasons), optional | `climbing_interest` = started (that season) |
| 9 | How do you usually find groups? | Choice: group finder, a regular group, guild, friends, a mix, other | `social_mode` (general) |
| 10 | Your characters (Name-Realm, one per line; * after current main) | Paragraph | `characters` + `player_characters` (how = `self_reported`) |
| 11 | May we look up these characters on Raider.IO and link them to your answers? | Choice: yes / no | consent flag for lookups |
| 12a | How much Mythic+ did you play each season? | Grid: rows = seasons; columns = didn't play, a little, regularly, a lot | `mplus_volume` (per season) |
| 12b | How did your Mythic+ play change over each season? | Grid: rows = seasons; columns = about the same throughout, more as the season went on, less as the season went on, stopped before the season ended, started partway through | `persistence` (per season) |
| 13 | In seasons where you played less or stopped, what contributed? | Checkboxes (shuffled): got the rewards or rating I wanted; gear felt done; switched to other content; other games or hobbies; less free time; didn't enjoy that season's dungeons or affixes; class or spec changes; changes in my group or guild; hard to find groups; burned out; doesn't apply; other | `stop_reason` (several = possibly a chain; order comes from the free text, not from this list) |
| 14 | Is there a season that stands out to you, good or bad? What made it that way? | Paragraph | free text: classifier training data |
| 15 | Anything from earlier seasons worth knowing? | Paragraph | free text; era notes |
| 16 | How do you feel about random loot in Mythic+? | Choice: enjoy random drops / prefer guaranteed or choosable rewards / a mix / doesn't matter much | `loot_attitude` |
| 17 | How has Mythic+ changed for you over the years? | Paragraph | free text; `season_rules.notes` ideas |
| 18 | May we quote your written answers anonymously on the blog? | Choice: yes / no | quote permission |
| 19 | Optional: Discord or BattleTag if you're OK with a follow-up | Short text | stored separately from answers; never published |

### Other design notes

- **No score questions.** Scores come from Raider.IO, which is more reliable than memory.
- **Every question except consent is optional**, so a partial history is still useful.
- Answers are `labeled_by = 'respondent (self-reported)'`, the same standard as the owner's gold
  labels.

## After the pilot

- Look at skipped and confusing questions, and how long people took.
- Check the answers for signs of priming (e.g. many respondents echoing the same phrasing).
- Import: export responses as CSV; a small importer turns them into `players`,
  `player_characters` and `gold_labels` (to be written once the questions settle).
- Then build the Battle.net-login version (verified characters and alts).
