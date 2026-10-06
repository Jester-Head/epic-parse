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
 * Question numbers and codebook: docs/survey/pilot-survey.md
 */
const CONTACT = 'epicparse.research@gmail.com';
const FORM_ID = '1BIzxcvAv9efqVwX-XY-mfw3M57CdYhpWsQ-u02l5pSU';  // the pilot form created on 2026-10-05

const SEASONS = ['TWW Season 1', 'TWW Season 2', 'TWW Season 3', 'Midnight Season 1', 'Midnight Season 2 (so far)'];
const ALL_SEASONS = ['Legion or earlier', 'BfA Season 1', 'BfA Season 2', 'BfA Season 3', 'BfA Season 4',
  'Shadowlands Season 1', 'Shadowlands Season 2', 'Shadowlands Season 3', 'Shadowlands Season 4',
  'Dragonflight Season 1', 'Dragonflight Season 2', 'Dragonflight Season 3', 'Dragonflight Season 4',
  'TWW Season 1', 'TWW Season 2', 'TWW Season 3', 'Midnight Season 1', 'Midnight Season 2'];
const CONTENT = ['Mythic+', 'Raiding', 'Rated PvP', 'Delves / solo content', 'RP, collecting or transmog', 'Other'];

function createSurvey() {
  const form = FormApp.create('Your Mythic+ experience (Epic-Parse pilot survey)');
  build(form);
}

function rebuildSurvey() {
  const form = FormApp.openById(FORM_ID);
  if (form.getResponses().length > 0) {
    throw new Error('This form already has responses. Rebuilding would break them: use createSurvey() for a new form instead.');
  }
  form.getItems().forEach(function (item) { form.deleteItem(item); });
  form.setTitle('Your Mythic+ experience (Epic-Parse pilot survey)');
  build(form);
}

function build(form) {
  form.setDescription(
    'A short survey about how different players experience Mythic+: what matters to them, how they play, ' +
    'and how that changes over a season. About 10 minutes. Results may appear in blog posts.');
  form.setCollectEmail(false);
  form.setAllowResponseEdits(true);
  form.setLimitOneResponsePerUser(false);
  form.setProgressBar(true);
  form.setConfirmationMessage(
    'Thank you! If you want to change or remove your answers later, keep your edit link or contact ' + CONTACT + '.');

  // 0. Consent
  form.addSectionHeaderItem().setTitle('Before you start').setHelpText(
    '• Taking part is voluntary, and every question except this one is optional.\n' +
    '• There are no right answers. Every play style is welcome, including people who rarely run keys.\n' +
    '• Results are only published in aggregate. Nothing is published with your character names.\n' +
    '• Your written answers are only quoted (anonymously) if you say yes at the end.\n' +
    '• If you list characters, their public Mythic+ and raid data may be looked up on Raider.IO ' +
    'and linked to your answers, only if you allow it.\n' +
    '• You can ask for your answers to be deleted at any time: ' + CONTACT + '. ' +
    'Saving your response\'s edit link also lets you change it later.\n' +
    '• No email address is collected.');
  form.addMultipleChoiceItem().setTitle("I've read the above and agree to take part")
    .setChoiceValues(['I agree']).setRequired(true);

  // About you
  form.addPageBreakItem().setTitle('About you');
  form.addMultipleChoiceItem().setTitle('Which region do you mostly play in?')
    .setChoiceValues(['US', 'EU', 'Oceanic', 'KR', 'TW']);
  form.addListItem().setTitle('When did you start running Mythic+ regularly?')
    .setChoiceValues(ALL_SEASONS.concat(['I rarely or never run Mythic+']));
  form.addCheckboxItem().setTitle('What do you mostly play?').setChoiceValues(CONTENT);
  form.addMultipleChoiceItem().setTitle('Which one matters most to you?').setChoiceValues(CONTENT);
  form.addCheckboxItem().setTitle('What role(s) do you play in Mythic+?')
    .setChoiceValues(['Tank', 'Healer', 'DPS']);
  form.addGridItem().setTitle('How important are these to you when you play Mythic+?')
    .setRows(['Getting gear', 'Earning rewards (titles, mounts, achievements)', 'Raising my rating',
      'Improving my own play', 'Playing with friends or my guild', 'Enjoying the dungeons themselves',
      'Competing with other players', 'Something to do in a short session'])
    .setColumns(['Not important', 'A little', 'Important', 'Very important']);
  form.addMultipleChoiceItem().setTitle('Do you set rating or achievement goals in Mythic+?')
    .setHelpText('For example, a score, a key level, or an achievement such as Keystone Master or Hero.')
    .setChoiceValues(['Never', 'In some seasons', 'In most seasons']);
  form.addListItem().setTitle('If you do, since about when?').setChoiceValues(ALL_SEASONS);
  form.addMultipleChoiceItem().setTitle('How do you usually find groups?')
    .setChoiceValues(['Group finder (pugs)', 'A regular group', 'Guild', 'Friends', 'A mix'])
    .showOtherOption(true);

  // Characters
  form.addPageBreakItem().setTitle('Your characters')
    .setHelpText('Optional. Scores come from Raider.IO, so there are no score questions.');
  form.addParagraphTextItem()
    .setTitle('Your characters (Name-Realm, one per line). Put * after your current main.')
    .setHelpText('Example:\nMychar-Area 52 *\nMyalt-Stormrage');
  form.addMultipleChoiceItem()
    .setTitle('May we look up these characters on Raider.IO and link them to your answers?')
    .setChoiceValues(['Yes', 'No']);

  // Season history
  form.addPageBreakItem().setTitle('Season by season')
    .setHelpText('Fill in only the seasons you remember. Leave a row empty if unsure.');
  form.addGridItem().setTitle('How much Mythic+ did you play each season?').setRows(SEASONS)
    .setColumns(["Didn't play", 'A little', 'Regularly', 'A lot']);
  form.addGridItem().setTitle('How did your Mythic+ play change over each season?').setRows(SEASONS)
    .setColumns(['About the same throughout', 'More as the season went on', 'Less as the season went on',
      'Stopped before the season ended', 'Started partway through']);
  form.addCheckboxItem()
    .setTitle('In seasons where you played less Mythic+ or stopped, what contributed? Tick any that apply.')
    .setChoiceValues(['I got the rewards or rating I wanted', 'My gear felt done', 'I switched to other content (raid, PvP, alts…)',
      'Other games or hobbies', 'Less free time', "I didn't enjoy that season's dungeons or affixes",
      'Changes to my class or spec', 'Changes in my group or guild', 'It was hard to find groups', 'I felt burned out',
      "Doesn't apply: I didn't play less"])
    .showOtherOption(true);

  // In your own words
  form.addPageBreakItem().setTitle('In your own words');
  form.addParagraphTextItem()
    .setTitle('Is there a season that stands out to you, good or bad? What made it that way?');
  form.addParagraphTextItem()
    .setTitle('Anything from earlier seasons (Legion, BfA, Shadowlands, Dragonflight) worth knowing?');
  form.addMultipleChoiceItem().setTitle('How do you feel about random loot in Mythic+?')
    .setChoiceValues(['I enjoy the excitement of random drops',
      'I prefer guaranteed or choosable rewards (vault, crests, vendors)', 'I like a mix of both',
      "It doesn't matter much to me"]);
  form.addParagraphTextItem().setTitle('How has Mythic+ changed for you over the years?');

  // Wrap up
  form.addPageBreakItem().setTitle('Last two questions');
  form.addMultipleChoiceItem().setTitle('May we quote your written answers anonymously on the blog?')
    .setChoiceValues(['Yes', 'No']);
  form.addTextItem().setTitle("Optional: Discord or BattleTag, if you're OK with a follow-up question")
    .setHelpText('Never published. Kept separate from your answers.');

  Logger.log('Edit link: ' + form.getEditUrl());
  Logger.log('Share link: ' + form.getPublishedUrl());
}
