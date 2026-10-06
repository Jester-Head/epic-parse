/**
 * Builds the Epic-Parse pilot survey as a Google Form.
 *
 * 1. Go to https://script.google.com and create a new project.
 * 2. Paste this file, fill in CONTACT below, and run createSurvey().
 * 3. Approve the permissions (it creates one form in your Google Drive).
 * 4. The log shows the edit link and the public link to share.
 *
 * Question numbers and codebook: docs/survey/pilot-survey.md
 */
const CONTACT = '[TODO: contact for deletion requests, e.g. a Discord handle or email]';

const SEASONS = ['TWW Season 1', 'TWW Season 2', 'TWW Season 3', 'Midnight Season 1', 'Midnight Season 2 (so far)'];
const ALL_SEASONS = ['Legion or earlier', 'BfA Season 1', 'BfA Season 2', 'BfA Season 3', 'BfA Season 4',
  'Shadowlands Season 1', 'Shadowlands Season 2', 'Shadowlands Season 3', 'Shadowlands Season 4',
  'Dragonflight Season 1', 'Dragonflight Season 2', 'Dragonflight Season 3', 'Dragonflight Season 4',
  'TWW Season 1', 'TWW Season 2', 'TWW Season 3', 'Midnight Season 1', 'Midnight Season 2'];
const CONTENT = ['Mythic+', 'Raiding', 'Rated PvP', 'Delves / solo content', 'RP, collecting or transmog', 'Other'];

function createSurvey() {
  const form = FormApp.create('Your Mythic+ history (Epic-Parse pilot survey)');
  form.setDescription(
    'A short survey about how different kinds of WoW players experience Mythic+: what they aim for, ' +
    'how they find groups, and why they stop pushing. About 10 minutes. Results may appear in blog posts.');
  form.setCollectEmail(false);
  form.setAllowResponseEdits(true);
  form.setLimitOneResponsePerUser(false);
  form.setProgressBar(true);
  form.setConfirmationMessage(
    'Thank you! If you want to change or remove your answers later, keep your edit link or contact ' + CONTACT + '.');

  // 0. Consent
  form.addSectionHeaderItem().setTitle('Before you start').setHelpText(
    '• Taking part is voluntary, and every question except this one is optional.\n' +
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
  form.addListItem().setTitle('When did you start doing Mythic+ regularly?')
    .setChoiceValues(ALL_SEASONS.concat(["I don't do Mythic+ regularly"]));
  form.addMultipleChoiceItem().setTitle('Have you ever made a real effort to push your rating?')
    .setChoiceValues(['Yes, most seasons', 'Yes, in some seasons',
      'Not really — I run keys for gear, fun or with friends', 'No']);
  form.addListItem().setTitle('If so, when did you first push?')
    .setChoiceValues(ALL_SEASONS);
  form.addCheckboxItem().setTitle('What do you mostly play?').setChoiceValues(CONTENT);
  form.addMultipleChoiceItem().setTitle('Which one matters most to you?').setChoiceValues(CONTENT);
  form.addCheckboxItem().setTitle('What role(s) do you play in Mythic+ nowadays?')
    .setChoiceValues(['Tank', 'Healer', 'DPS']);
  form.addScaleItem().setTitle('Why do you play Mythic+?').setBounds(1, 5)
    .setLabels('For fun / the experience', 'To climb / for results');
  form.addMultipleChoiceItem().setTitle('How do you usually form groups?')
    .setChoiceValues(['Mostly pugs (group finder)', 'A static / premade group', 'Guild groups', 'Friends', 'A mix']);

  // Characters
  form.addPageBreakItem().setTitle('Your characters')
    .setHelpText('Optional. Scores come from Raider.IO, so there are no score questions.');
  form.addParagraphTextItem()
    .setTitle('Your characters (Name-Realm, one per line). Put * after this season\'s main.')
    .setHelpText('Example:\nMychar-Area 52 *\nMyalt-Stormrage');
  form.addMultipleChoiceItem()
    .setTitle('May we look up these characters on Raider.IO and link them to your answers?')
    .setChoiceValues(['Yes', 'No']);

  // Season history
  form.addPageBreakItem().setTitle('Season by season')
    .setHelpText('Fill in only the seasons you remember. Leave a row empty if unsure.');
  form.addGridItem().setTitle('How did you group each season?').setRows(SEASONS)
    .setColumns(["Didn't play M+", 'Mostly pugs', 'Static / premade', 'Static, then pugs', 'A mix']);
  form.addGridItem().setTitle('How did each season go for you?').setRows(SEASONS)
    .setColumns(["Didn't play M+", "Played casually (wasn't pushing)", 'Pushed to the end',
      'Pushed, then stopped partway', 'Pushed, then stopped early', 'Pushed, then stopped very early']);
  form.addCheckboxGridItem().setTitle('If you stopped: what happened? Tick everything that applied.')
    .setHelpText('Reasons often chain together (for example: the group fell apart, then pugging, then burnout).')
    .setRows(SEASONS)
    .setColumns(['Group fell apart', 'Pugging got too frustrating', 'Burned out', 'Lost interest',
      'Life got busy', 'Hit my goal', 'Gear / loot luck', 'Other']);

  // Your story
  form.addPageBreakItem().setTitle('In your own words');
  form.addParagraphTextItem()
    .setTitle('Tell us about a season where you stopped, or kept going. What happened?');
  form.addParagraphTextItem()
    .setTitle('Anything from earlier seasons (Legion, BfA, Shadowlands, Dragonflight) worth knowing?');
  form.addMultipleChoiceItem().setTitle('How do you feel about loot randomness?')
    .setChoiceValues(['I love the thrill of random drops',
      'I prefer guaranteed rewards (vault, crests, catch-up currencies)', 'I like a mix', "I don't mind either way"]);
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
