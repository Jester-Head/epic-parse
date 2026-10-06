/**
 * Builds the Epic-Parse pilot survey as a Google Form.
 *
 * Fresh form:      run createSurvey()   (creates a new form in your Google Drive)
 * Existing form:   run rebuildSurvey()  (clears FORM_ID's questions and rebuilds them,
 *                                         keeping the same links; only before responses exist)
 *
 * In the Apps Script editor, pick the function in the dropdown next to Run.
 * The log shows the edit link and the share link.
 *
 * Structure, scales and codebook: docs/survey/pilot-survey.md
 */
const CONTACT = 'epicparse.research@gmail.com';
const FORM_ID = '1BIzxcvAv9efqVwX-XY-mfw3M57CdYhpWsQ-u02l5pSU';  // the pilot form created on 2026-10-05
const TITLE = 'Your Mythic+ experience (Epic-Parse pilot survey)';

// Standard response scales
const FREQUENCY = ['Never', 'Rarely', 'Sometimes', 'Often', 'Very often'];
// Short labels so grid columns aren't cut off; the question text carries the full meaning.
const IMPORTANCE = ['Not at all', 'Slightly', 'Moderately', 'Very', 'Extremely', 'N/A'];
const AGREEMENT = ['Strongly disagree', 'Disagree', 'Neither agree nor disagree', 'Agree', 'Strongly agree'];
// Breaks at 1 / 4 / 8 follow the Great Vault's three Mythic+ slots (8 keys fill it); 8–10 separates
// "about a full vault" from playing well past it.
const KEYS_PER_WEEK = ['None', '1–3', '4–7', '8–10', '11–15', '16+', 'Not sure'];

// Patch periods within each season (US release dates from epic_parse/wow_patches.py;
// season dates from Raider.IO). Rows of the per-season grids.
const SEASON_PATCHES = [
  ['The War Within Season 1', ["11.0.2 (Sep–Oct '24)", "11.0.5 (Oct–Dec '24)", "11.0.7 (Dec '24–Feb '25)"]],
  ['The War Within Season 2', ["11.1.0 (Mar–Apr '25)", "11.1.5 (Apr–Jun '25)", "11.1.7 (Jun–Aug '25)"]],
  ['The War Within Season 3', ["11.2.0 (Aug–Oct '25)", "11.2.5 (Oct–Dec '25)", "11.2.7 (Dec '25–Jan '26)",
    "Pre-patch (Jan–Mar '26)"]],
  ['Midnight Season 1', ["12.0.1 (Mar–Apr '26)", "12.0.5 (Apr–Jun '26)", "12.0.7 (Jun–Aug '26)"]],
  ['Midnight Season 2 (so far)', ["12.1.0 (Aug '26–now)"]],
];
const ALL_SEASONS = ['Legion or earlier', 'BfA Season 1', 'BfA Season 2', 'BfA Season 3', 'BfA Season 4',
  'Shadowlands Season 1', 'Shadowlands Season 2', 'Shadowlands Season 3', 'Shadowlands Season 4',
  'Dragonflight Season 1', 'Dragonflight Season 2', 'Dragonflight Season 3', 'Dragonflight Season 4',
  'TWW Season 1', 'TWW Season 2', 'TWW Season 3', 'Midnight Season 1', 'Midnight Season 2'];
const ACTIVITIES = ['Mythic+', 'Raiding', 'Rated PvP', 'Delves', 'Questing / world', 'Housing', 'Collecting',
  'Gold making', 'Role-play', 'Alts / leveling'];
const REASONS = ['I reached the rewards or rating I wanted', 'My gear felt complete',
  'I switched to other content (raiding, PvP, alts, etc.)', 'Other games or hobbies', 'Less free time',
  "I didn't enjoy that season's dungeons or affixes", 'Changes to my class or spec',
  'Changes in my group or guild', 'It was hard to find groups', 'I felt burned out'];

function createSurvey() {
  build(FormApp.create(TITLE));
}

function rebuildSurvey() {
  const form = FormApp.openById(FORM_ID);
  if (form.getResponses().length > 0) {
    throw new Error('This form already has responses. Rebuilding would break them: use createSurvey() for a new form instead.');
  }
  form.getItems().forEach(function (item) { form.deleteItem(item); });
  form.setTitle(TITLE);
  build(form);
}

function build(form) {
  form.setDescription(
    'A survey about how players experience World of Warcraft, with a focus on Mythic+: what matters to them, ' +
    'how they play, and how that changes over a season. It takes about 10 minutes. ' +
    'Results may appear in blog posts.');
  form.setCollectEmail(false);
  form.setAllowResponseEdits(true);
  form.setLimitOneResponsePerUser(false);
  form.setProgressBar(true);
  form.setConfirmationMessage(
    'Thank you! If you want to change or remove your answers later, keep your edit link or contact ' + CONTACT + '.');

  // Section 1: Consent
  form.addSectionHeaderItem().setTitle('About this survey').setHelpText(
    '• Taking part is voluntary, and every question except this one is optional.\n' +
    '• There are no right or wrong answers. Every play style is welcome.\n' +
    '• Results are only published in aggregate. Nothing is published with your character names.\n' +
    '• Written answers are only quoted (anonymously) if you give permission at the end.\n' +
    '• If you list characters, their public Mythic+ and raid data may be looked up on Raider.IO ' +
    'and linked to your answers, only if you allow it.\n' +
    '• You can ask for your answers to be deleted at any time: ' + CONTACT + '. ' +
    'Saving your response\'s edit link also lets you change it later.\n' +
    '• No email address is collected.');
  form.addMultipleChoiceItem().setTitle('I have read the information above and agree to take part.')
    .setChoiceValues(['I agree']).setRequired(true);

  // Section 2: How you play (everyone)
  form.addPageBreakItem().setTitle('How you play')
    .setHelpText('Think about the last two years of World of Warcraft.');
  form.addGridItem().setTitle('1. How often do you do each of these activities?')
    .setHelpText('Collecting = mounts, pets, transmog or achievements. Gold making = professions, the auction house, farming.')
    .setRows(ACTIVITIES).setColumns(FREQUENCY);
  form.addMultipleChoiceItem().setTitle('2. Which activity matters most to you?')
    .setChoiceValues(ACTIVITIES).showOtherOption(true);

  // Screener: people who don't run Mythic+ skip the Mythic+ sections
  const screener = form.addMultipleChoiceItem()
    .setTitle('3. In the past two years, how often have you run Mythic+ dungeons?');

  // Section 3: Mythic+
  const mplusPage = form.addPageBreakItem().setTitle('Mythic+');
  form.addCheckboxItem().setTitle('4. Which role(s) do you play in Mythic+?')
    .setChoiceValues(['Tank', 'Healer', 'Damage']);
  form.addGridItem().setTitle('5. How important is each of the following to you when you play Mythic+?')
    .setHelpText('Rewards = titles, mounts or achievements. N/A = does not apply to you.')
    .setRows(['Getting gear', 'Earning rewards', 'Raising my rating', 'Improving my play',
      'Friends or guild', 'Enjoying the dungeons', 'Competing with others', 'Short play sessions'])
    .setColumns(IMPORTANCE);
  form.addMultipleChoiceItem().setTitle('6. How often do you set a rating or achievement goal for a season?')
    .setHelpText('For example, a score, a key level, or an achievement such as Keystone Master or Hero.')
    .setChoiceValues(['Never', 'In some seasons', 'In most seasons', 'In every season']);
  form.addMultipleChoiceItem().setTitle('7. How do you usually find Mythic+ groups?')
    .setChoiceValues(['Group finder (pick-up groups)', 'A regular group', 'Guild', 'Friends', 'A mix of these'])
    .showOtherOption(true);

  // Section 4: Season history
  form.addPageBreakItem().setTitle('Season by season')
    .setHelpText('8. For each patch, roughly how many Mythic+ keys did you run per week? ' +
      'The first patch listed is the start of the season. For reference, 8 keys fill the Great Vault. ' +
      'Choose "Not sure" or leave a row empty if you don\'t remember.');
  SEASON_PATCHES.forEach(function (season) {
    form.addGridItem().setTitle(season[0]).setRows(season[1]).setColumns(KEYS_PER_WEEK);
  });

  // Section 5: Changes during a season
  form.addPageBreakItem().setTitle('Changes during a season');
  form.addCheckboxItem()
    .setTitle('9. Think about times you ran fewer keys later in a season than earlier, or stopped. ' +
      'Which of the following contributed? Select all that apply.')
    .setChoiceValues(REASONS.concat(['Not applicable: this has not happened to me']))
    .showOtherOption(true);
  form.addMultipleChoiceItem().setTitle('10. Of those, which was the main reason?')
    .setChoiceValues(REASONS.concat(['Not applicable']))
    .showOtherOption(true);

  // Section 6: Opinions
  form.addPageBreakItem().setTitle('Your views');
  form.addSectionHeaderItem().setTitle('11. How much do you agree or disagree with each statement?');
  // Separate questions rather than a grid: long statements get cut off in grid rows on phones.
  [['11a', 'I enjoy the excitement of random loot drops.'],
   ['11b', 'I prefer rewards I can choose (Great Vault, crests, vendors).'],
   ['11c', 'Mythic+ is more fun with people I know.'],
   ['11d', 'Mythic+ asks for more time than I want to give it.'],
   ['11e', 'The rewards for Mythic+ are worth the effort.']].forEach(function (statement) {
    form.addMultipleChoiceItem().setTitle(statement[0] + '. ' + statement[1]).setChoiceValues(AGREEMENT);
  });

  // Section 7: In your own words
  form.addPageBreakItem().setTitle('In your own words');
  form.addParagraphTextItem()
    .setTitle('12. Is there a season that stands out to you, good or bad? What made it that way?');
  form.addParagraphTextItem().setTitle('13. How has Mythic+ changed for you over the years?');
  form.addParagraphTextItem()
    .setTitle('14. Anything from earlier seasons (Legion, BfA, Shadowlands, Dragonflight) worth knowing?');

  // Section 8: About you (background questions last)
  const aboutPage = form.addPageBreakItem().setTitle('About you');
  form.addMultipleChoiceItem().setTitle('15. Which region do you mostly play in?')
    .setChoiceValues(['US', 'EU', 'Oceanic', 'KR', 'TW']);
  form.addListItem().setTitle('16. When did you start running Mythic+ regularly?')
    .setChoiceValues(ALL_SEASONS.concat(['I have not run Mythic+ regularly']));
  form.addParagraphTextItem()
    .setTitle('17. Optional: your characters (Name-Realm, one per line). Put * after your current main.')
    .setHelpText('Example:\nMychar-Area 52 *\nMyalt-Stormrage');
  form.addMultipleChoiceItem()
    .setTitle('18. May we look up these characters on Raider.IO and link them to your answers?')
    .setChoiceValues(['Yes', 'No']);

  // Section 9: Wrap up
  form.addPageBreakItem().setTitle('Before you finish');
  form.addMultipleChoiceItem().setTitle('19. May we quote your written answers anonymously on the blog?')
    .setChoiceValues(['Yes', 'No']);
  form.addTextItem().setTitle("20. Optional: Discord or BattleTag, if you're open to a follow-up question")
    .setHelpText('Never published. Kept separate from your answers.');

  // Screener branching (needs the target pages to exist first)
  screener.setChoices(FREQUENCY.map(function (value) {
    return value === 'Never'
      ? screener.createChoice(value, aboutPage)
      : screener.createChoice(value, mplusPage);
  }));

  Logger.log('Edit link: ' + form.getEditUrl());
  Logger.log('Share link: ' + form.getPublishedUrl());
}
