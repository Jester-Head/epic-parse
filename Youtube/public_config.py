# Rename this file to config.py and fill in the necessary information
# This file contains the configuration for the YouTube API and MongoDB

API_KEYS = ['your_api_keys']
MONGO_URI = "mongodb://your_mongo_uri"
MONGO_DB = "your_mongo_db"
MONGO_COLL = "your_mongo_collection"

# Battle for Azeroth release date
CUTOFF_DATE = '2018-08-14T00:00:00Z'

KEYWORDS = {'BFA', 'Battle for Azeroth', 'Blizzard', 'Cata', 'Cataclysm', 'Cinematic', 'Classic', 'Classic WoW',
            'Cutscene', 'Dragonflight', 'Dungeon',
            'Legion', 'Lore', 'MDI', 'Meta', 'Mists of Pandaria', 'MoP', 'Mythic', 'Mythic+', 'Patch', 'PvE',
            'PvP', 'RWF', 'Raid', 'Retail', 'SL', 'Shadowlands', 'Story', 'TBC', 'TWW', 'The Burning Crusade',
            'The War Within', 'Vanilla', 'Warcraft', 'Warlords of Draenor', 'WoD', 'WoW', 'WoW Classic',
            'World of Lorecraft',
            'World of Warcraft', 'World Soul Saga' 'WotLK', 'Wrath of the Lich King'}

LOG_CONFIG_PATH = 'Youtube/logging_config.json'

CHANNELS = {
    # ---------------------- A ---------------------- #
    "Accolonn": {
        "handle": "@theaccolonn",
        "channel_id": "UCiYLmBHW28jyTt0vBUFuK5A",
        "only_wow": True,
        "version": "retail",
        "tags": ["lore", "analysis", "general"],
        "outdated": False,
    },
    "Asmongold": {
        "handle": "@asmontv",
        "channel_id": "UCQeRaTukNYft1_6AZPACnog",
        "only_wow": False,
        "version": "both",
        "tags": ["react", "general", "variety"],
        "outdated": False,
    },
    "AutomaticJak": {
        "handle": "@automaticjak",
        "channel_id": "UCzFK2Yn2gu20AWV1raNbiOQ",
        "only_wow": True,
        "version": "retail",
        "tags": ["healer", "high-end", "guides"],
        "outdated": False,
    },
    # ---------------------- B ---------------------- #
    "Bajheera": {
        "handle": "@BajheeraWOW",
        "channel_id": "UCiWFHBP-d3GNxg_U3NYJKyg",
        "only_wow": False,
        "version": "both",
        "tags": ["pvp", "guides", "stream"],
        "outdated": False,
    },
    "Bellular Warcraft": {
        "handle": "@bellulargaming",
        "channel_id": "UCwiaPYufmQOq5F1TI-FzQhw",
        "only_wow": True,
        "version": "retail",
        "tags": ["news", "lore", "analysis"],
        "outdated": False,
    },
    # ---------------------- C ---------------------- #

    "Chanimal": {
        "handle": "@Chanimal",
        "channel_id": "UCVdEqJ5uwhF-94HOM61Ljow",
        "only_wow": True,
        "version": "both",
        "tags": ["pvp", "high-end"],
        "outdated": False,
    },
    # ---------------------- D ---------------------- #
    "Dalaran Gaming": {
        "handle": "@dalarangaming",
        "channel_id": "UCobP8-RBCEPh4Bzsm02BNFQ",
        "only_wow": True,
        "version": "retail",
        "tags": ["pve", "pvp", "guides"],
        "outdated": False,
    },
    "Dorki": {
        "handle": "@dorki",
        "channel_id": "UC_XwQjQkSEzp_m55-D2eJqA",
        "only_wow": True,
        "version": "retail",
        "tags": ["tank", "high-end", "mplus"],
        "outdated": False,
    },
    "Dratnos": {
        "handle": "@dratnos",
        "channel_id": "UCI-FRT31iq5_xoFG6ngNcpw",
        "only_wow": True,
        "version": "retail",
        "tags": ["analysis", "high-end", "mplus"],
        "outdated": False,
    },
    "Dvalin Gaming": {
        "handle": "@dvalingaming",
        "channel_id": "UCxIAxsveK2KOdy_Wh4WcUsQ",
        "only_wow": True,
        "version": "both",
        "tags": ["general", "guides"],
        "outdated": False,
    },
    # ---------------------- E ---------------------- #
    "Echo": {
        "handle": "@EchoEsports",
        "channel_id": "UCUiTgN-8IS9btyZN01ioy_A",
        "only_wow": True,
        "version": "retail",
        "tags": ["high-end", "raid", "mplus", "esports"],
        "outdated": False,
    },
    # ---------------------- G ---------------------- #
    "Gingi": {
        "handle": "@gingitv",
        "channel_id": "UCvV80raFP2R7X0yxH9iJjog",
        "only_wow": True,
        "version": "retail",
        "tags": ["high-end", "dps"],
        "outdated": False,
    },
    "Growl": {
        "handle": "@yumytv",
        "channel_id": "UCwxbtziyebIaVN2XS0wVITQ",
        "only_wow": True,
        "version": "retail",
        "tags": ["healer", "high-end", "mplus"],
        "outdated": False,
    },
    # ---------------------- H ---------------------- #
    "Hazelnuttygames": {
        "handle": "@hazelnuttygames",
        "channel_id": "UCMGVp_GnkhHZROIfRdXpo4Q",
        "only_wow": False,
        "version": "retail",
        "tags": ["general", "guides"],
        "outdated": False,
    },
    "Hopeful": {
        "handle": "@Hopefulqt",
        "channel_id": "UCuB-vgh4HQyvVFVYAmO7SrQ",
        "only_wow": True,
        "version": "retail",
        "tags": ["high-end", "dps"],
        "outdated": False,
    },
    # ---------------------- I ---------------------- #

    # ---------------------- L ---------------------- #
    "LBNinja7": {
        "handle": "@lbninja7",
        "channel_id": "UC8n5zBJRVelcB4wWeSmmIGA",
        "only_wow": True,
        "version": "retail",
        "tags": ["guides", "general", "healer"],
        "outdated": False,
    },
    # ---------------------- M ---------------------- #
    "Mad Skillz": {
        "handle": "@madskillz",
        "channel_id": "UCGzoxWhHOV24jsDyAHv5N5g",
        "only_wow": True,
        "version": "retail",
        "tags": ["guides", "high-end", "healer"],
        "outdated": False,
    },
    "MarcelianOnline": {
        "handle": "@marcelianonline",
        "channel_id": "UCyf0SCaSGhKpvI_azfvidUw",
        "only_wow": True,
        "version": "retail",
        "tags": ["general", "guides"],
        "outdated": False,
    },
    "Maximum": {
        "handle": "@limitmaximum",
        "channel_id": "UCW7BPvFaeiqMCYcNkOAhkxQ",
        "only_wow": True,
        "version": "retail",
        "tags": ["high-end", "raid"],
        "outdated": False,
    },
    "Method": {
        "handle": "@Methodgg",
        "channel_id": "UCJRsrXVPx3awXJX6WkFz4Dw",
        "only_wow": True,
        "version": "retail",
        "tags": ["high-end", "guides"],
        "outdated": False,
    },
    "MrGM": {
        "handle": "@mrgm",
        "channel_id": "UCjZfZo5bI5-ITtRPBPZkIhw",
        "only_wow": True,
        "version": "retail",
        "tags": ["news", "general"],
        "outdated": False,
    },
    "MoreMrGM": {
        "handle": "@moremrgm",
        "channel_id": "UCj-Tw9GZaRL8geA6HvOhVug",
        "only_wow": True,
        "version": "retail",
        "tags": ["news", "general"],
        "outdated": False,
    },
    # ---------------------- N ---------------------- #
    "Naguura": {
        "handle": "@naguura",
        "channel_id": "UC9hQOoOtN-xaUjPn1jWzsWQ",
        "only_wow": False,
        "version": "retail",
        "tags": ["mplus", "high-end", "dps"],
        "outdated": False,
    },
    "Nobbel87": {
        "handle": "@nobbel87",
        "channel_id": "UCX34tk-noBVC4WVC9qQGyMw",
        "only_wow": True,
        "version": "retail",
        "tags": ["lore"],
        "outdated": False,
    },
    # ---------------------- O ---------------------- #

    # ---------------------- P ---------------------- #
    "Platinum WoW": {
        "handle": "@platinumwow",
        "channel_id": "UCTqUnnRlK-n44W6xPlx6JVQ",
        "only_wow": True,
        "version": "retail",
        "tags": ["lore"],
        "outdated": False,
    },
    "Preach Gaming": {
        "handle": "@preachgaming",
        "channel_id": "UCXJL3ST-O0J3nqzQyPJtpNg",
        "only_wow": False,
        "version": "retail",
        "tags": ["high-end", "analysis", "variety"],
        "outdated": False,
    },
    # ---------------------- R ---------------------- #
    "Ready Check Pull": {
        "handle": "@ReadyCheckPull",
        "channel_id": "UCB1XBuXDQkOLEIvvYTh0SEg",
        "only_wow": True,
        "version": "retail",
        "tags": ["podcast", "high-end"],
        "outdated": False,
    },
    # ---------------------- S ---------------------- #
    "Samiccus": {
        "handle": "@samiccus",
        "channel_id": "UCmYY3o4EH-tPqxKosY8A_1Q",
        "only_wow": True,
        "version": "retail",
        "tags": ["general", "guides", "react"],
        "outdated": False,
    },
    "Scottejaye": {
        "handle": "@scottejaye",
        "channel_id": "UCyMNUoiD0vlmFtiriDtVI5Q",
        "only_wow": True,
        "version": "both",
        "tags": ["general", "analysis"],
        "outdated": False,
    },
    "SignsOfKelani": {
        "handle": "@signsofkelani",
        "channel_id": "UCsxidPdmPXDlsS3rn7arJsA",
        "only_wow": True,
        "version": "retail",
        "tags": ["news", "guides"],
        "outdated": False,
    },
    "Skill Capped WoW PvP Guides": {
        "handle": "@skillcappedwowpvp",
        "channel_id": "UCJfn3qHQ-Qy4xQDtSW3XT5Q",
        "only_wow": False,
        "version": "both",
        "tags": ["pvp", "guides", "high-end"],
        "outdated": False,
    },
    "SoulSoBreezy": {
        "handle": "@soulsobreezy",
        "channel_id": "UCeWYVWMwAuCDS4bgAhPoYwA",
        "only_wow": True,
        "version": "retail",
        "tags": ["news", "guides", "general"],
        "outdated": False,
    },
    "Sodapoppin": {
        "handle": "@sodapoppin",
        "channel_id": "UCtu2BCnJoFGRBOuIh570QWw",
        "only_wow": False,
        "version": "classic",
        "tags": ["classic", "variety", "react"],
        "outdated": False,
    },
    "Studen Albatroz": {
        "handle": "@StudenAlbatroz",
        "channel_id": "UCdstOzMVSXEjpBrNYyG_D0A",
        "only_wow": True,
        "version": "retail",
        "tags": ["gold", "guides"],
        "outdated": False,
    },
    # ---------------------- T ---------------------- #
    "Taliesin & Evitel": {  # For CLI type as: Taliesin" & "Evitel
        "handle": "@taliesinevitel",
        "channel_id": "UCiHdR2hAyFEXWCbNQgbu_Dg",
        "only_wow": True,
        "version": "retail",
        "tags": ["news", "lore", "general"],
        "outdated": False,
    },
    "Team Liquid": {
        "handle": "@TeamLiquidMMO",
        "channel_id": "UCANKeVhjyPq-BihWPg_k9Hw",
        "only_wow": True,
        "version": "retail",
        "tags": ["esports", "high-end"],
        "outdated": False,
    },
    "Tettles": {
        "handle": "@tettles",
        "channel_id": "UCQxhna2XRWA_Pts6abbpEIg",
        "only_wow": True,
        "version": "retail",
        "tags": ["high-end", "analysis", "mplus"],
        "outdated": False,
    },
    "Towelliee": {
        "handle": "@towelliee",
        "channel_id": "UCNYxHRVNsWYli_i474i7_BA",
        "only_wow": False,
        "version": "retail",
        "tags": ["tank", "high-end", "stream"],
        "outdated": False,
    },
    # ---------------------- V ---------------------- #
    "Venruki": {
        "handle": "@venruki",
        "channel_id": "UCL1w_mREZgr1NXpRz_i088Q",
        "only_wow": True,
        "version": "retail",
        "tags": ["pvp", "high-end", "guides"],
        "outdated": False,
    },
    # ---------------------- W ---------------------- #
    "Wille": {
        "handle": "@willemmo",
        "channel_id": "UC1haxYclmhXwa4FKFqYSaRw",
        "only_wow": True,
        "version": "classic",
        "tags": ["classic", "news", "guides"],
        "outdated": False,
    },
    "World of Warcraft": {
        "handle": "@warcraft",
        "channel_id": "UCbLj9QP9FAaHs_647QckGtg",
        "only_wow": True,
        "version": "both",
        "tags": ["official", "news", "general"],
        "outdated": False,
    },
    # ---------------------- X ---------------------- #
    "Xaryu": {
        "handle": "@xaryu",
        "channel_id": "UCAbaiKvP8kZfY706loT4ivg",
        "only_wow": False,
        "version": "classic",
        "tags": ["pvp", "classic", "stream"],
        "outdated": False,
    }
}
