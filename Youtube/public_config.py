# Rename this file to config.py and fill in the necessary information
# This file contains the configuration for the YouTube API and MongoDB

API_KEYS = ['your_api_keys']
MONGO_URI = "mongodb://your_mongo_uri"
MONGO_DB = "your_mongo_db"
MONGO_COLL = "your_mongo_collection"

# Battle for Azeroth release date
CUTOFF_DATE = '2018-08-14T00:00:00Z'

KEYWORDS = {'BFA', 'Battle for Azeroth', 'Blizzard', 'Cata', 'Cataclysm', 'Cinematic', 'Classic', 'Classic WoW', 'Cutscene', 'Dragonflight', 'Dungeon',
            'Legion', 'Lore', 'MDI', 'Meta', 'Mists of Pandaria', 'MoP', 'Mythic', 'Mythic+', 'Patch', 'PvE',
            'PvP', 'RWF', 'Raid', 'Retail', 'SL', 'Shadowlands', 'Story', 'TBC', 'TWW', 'The Burning Crusade',
            'The War Within', 'Vanilla', 'Warcraft', 'Warlords of Draenor', 'WoD', 'WoW', 'WoW Classic', 'World of Lorecraft',
            'World of Warcraft', 'World Soul Saga' 'WotLK', 'Wrath of the Lich King'}

LOG_CONFIG_PATH = 'Youtube/youtube_api_scripts/logging_config.json'

CHANNELS = {
    "World of Warcraft":  # Official WoW Channel
    {
        'handle': "@warcraft",
        'channel_id': 'UCbLj9QP9FAaHs_647QckGtg',
        'only_wow': True,
        'version': 'both'
    },
    "Bellular Warcraft":  # WoW News and Lore
    {
        "handle": "@bellulargaming",
        "channel_id": "UCwiaPYufmQOq5F1TI-FzQhw",
        "only_wow": True,
        "version": "retail"
    },
    'Asmongold':  # Reacts to various content. Influential,kinda controversial and critical of Blizzard
    {
        'handle': '@asmontv',
        'channel_id': 'UCQeRaTukNYft1_6AZPACnog',
        'only_wow': False,
        'version': 'both'
    },
    "Preach Gaming":  # Variety of content including WoW. Used to be high end raider
    {
        "handle": "@preachgaming",
        "channel_id": "UCXJL3ST-O0J3nqzQyPJtpNg",
        "only_wow": False,
        "version": "retail"
    },
    "Taliesin & Evitel":  # WoW News and Lore. Casual content
    {
        "handle": "@taliesinevitel",
        "channel_id": "UCiHdR2hAyFEXWCbNQgbu_Dg",
        "only_wow": False,
        "version": "retail"
    },
    "Nobbel87":  # Lore
    {
        "handle": "@nobbel87",
        "channel_id": "UCX34tk-noBVC4WVC9qQGyMw",
        "only_wow": True,
        "version": "retail"
    },
    "Hazelnuttygames":  # Not as active anymore
    {
        "handle": "@hazelnuttygames",
        "channel_id": "UCMGVp_GnkhHZROIfRdXpo4Q",
        "only_wow": False,
        "version": "retail"
    },
    "Accolonn":
    {
        "handle": "@theaccolonn",
        "channel_id": "UCiYLmBHW28jyTt0vBUFuK5A",
        "only_wow": True,
        "version": "retail"
    },
    "SoulSoBreezy":
    {
        "handle": "@soulsobreezy",
        "channel_id": "UCeWYVWMwAuCDS4bgAhPoYwA",
        "only_wow": True,
        "version": "retail"
    },
    "MrGM":
    {
        "handle": "@mrgm",
        "channel_id": "UCjZfZo5bI5-ITtRPBPZkIhw",
        "only_wow": True,
        "version": "retail"
    },
    'MoreMrGM': {
        'handle': '@moremrgm',
        'channel_id': 'UCj-Tw9GZaRL8geA6HvOhVug',
        'only_wow': True,
        'version': 'retail'
    },
    'Skill Capped WoW PvP Guides':  # PvP
    {
        'handle': '@skillcappedwowpvp',
        'channel_id': 'UCj4e6vS8dRJH8S47vJAU1Rw',
        'only_wow': False,
        'version': 'both'
    },
    "Towelliee":  # Tank POV
    {
        "handle": "@towelliee",
        "channel_id": "UCNYxHRVNsWYli_i474i7_BA",
        "only_wow": False,
        "version": "retail"
    },
    "Platinum WoW":
    {
        "handle": "@platinumwow",
        "channel_id": "UCTqUnnRlK-n44W6xPlx6JVQ",
        "only_wow": True,
        "version": "retail"
    },
    "SignsOfKelani":  # WoW news and guides
    {
        "handle": "@signsofkelani",
        "channel_id": "UCsxidPdmPXDlsS3rn7arJsA",
        "only_wow": True,
        "version": "retail"
    },
    "Dalaran Gaming":  # DPS PoV for PvE and PvP content
    {
        "handle": "@dalarangaming",
        "channel_id": "UCobP8-RBCEPh4Bzsm02BNFQ",
        "only_wow": True,
        "version": "retail"
    },
    "Venruki":
    {
        "handle": "@venruki",
        "channel_id": "UCL1w_mREZgr1NXpRz_i088Q",
        "only_wow": True,
        "version": "retail"
    },
    "Naguura":  # DPS M+ PoV
    {
        "handle": "@naguura",
        "channel_id": "UC9hQOoOtN-xaUjPn1jWzsWQ",
        "only_wow": False,
        "version": "retail"
    },
    "Dratnos":
    {
        "handle": "@dratnos",
        "channel_id": "UCI-FRT31iq5_xoFG6ngNcpw",
        "only_wow": True,
        "version": "retail"
    },
    "MarcelianOnline":
    {
        "handle": "@marcelianonline",
        "channel_id": "UCyf0SCaSGhKpvI_azfvidUw",
        "only_wow": True,
        "version": "retail"
    },
    "Maximum":  # Min/Maxing and Competitive PvE
    {
        "handle": "@limitmaximum",
        "channel_id": "UCW7BPvFaeiqMCYcNkOAhkxQ",
        "only_wow": True,
        "version": "retail"
    },
    "AutomaticJak":  # Min/Maxing Healer PoV
    {
        "handle": "@automaticjak",
        "channel_id": "UCzFK2Yn2gu20AWV1raNbiOQ",
        "only_wow": True,
        "version": "retail"
    },
    "Gingi":  # Min/Maxing DPS
    {
        "handle": "@gingitv",
        "channel_id": "UCvV80raFP2R7X0yxH9iJjog",
        "only_wow": True,
        "version": "retail"
    },
    "Growl":  # Min/Maxing Healer M+ PoV

    {
        "handle": "@yumytv",
        "channel_id": "UCwxbtziyebIaVN2XS0wVITQ",
        "only_wow": True,
        "version": "retail"
    },
    "Dorki":
    {
        "handle": "@dorki",
        "channel_id": "UC_XwQjQkSEzp_m55-D2eJqA",
        "only_wow": True,
        "version": "retail"
    },

    "Xaryu":  # Classic WoW/Hardcore
    {
        "handle": "@xaryu",
        "channel_id": "UCAbaiKvP8kZfY706loT4ivg",
        "only_wow": False,
        "version": "classic"
    },
    "Wille":  # General Classic WoW Content
    {
        "handle": "@willemmo",
        "channel_id": "UC1haxYclmhXwa4FKFqYSaRw",
        "only_wow": True,
        "version": "classic"
    },
    "Scottejaye":  # General WoW Content
    {
        "handle": "@scottejaye",
        "channel_id": "UCyMNUoiD0vlmFtiriDtVI5Q",
        "only_wow": True,
        "version": "both"
    }
}
# Comment section doesn't seem useful but keeping the info here just in case
# "Sodapoppin":
# {
#     "handle": "@sodapoppin",
#     "channel_id": "UCtu2BCnJoFGRBOuIh570QWw",
#     "only_wow": False,
#     "version": "classic"
# }
