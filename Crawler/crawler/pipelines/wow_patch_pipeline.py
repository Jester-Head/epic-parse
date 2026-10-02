from datetime import datetime

from itemadapter import ItemAdapter

retail_patches = {
    "Battle for Azeroth": {
        "start": "2018-08-14",
        "end": "2020-11-23",
        'patch': {
            "8.0.1": "2018-07-17",
            "8.1.0": "2018-12-11",
            "8.1.5": "2019-03-12",
            "8.2.0": "2019-06-25",
            "8.2.5": "2019-09-24",
            "8.3.0": "2020-01-14",
            "8.3.7": "2020-07-21",
        },
    },
    "Shadowlands": {
        "start": "2020-11-23",
        "end": "2022-10-25",
        "patch": {
            "9.0.1": "2020-10-13",
            "9.0.2": "2020-11-17",
            "9.1.0": "2021-06-29",
            "9.1.5": "2021-11-02",
            "9.2.0": "2022-02-22",
            "9.2.5": "2022-05-31",
            "9.2.7": "2022-08-16",
        }
    },
    "Dragonflight": {
        "start": "2022-10-25",
        "end": "2024-07-30",
        "patch": {
            "10.0.0": "2022-10-25",
            "10.0.2": "2022-11-15",
            "10.0.5": "2023-01-24",
            "10.0.7": "2023-03-21",
            "10.1.0": "2023-05-02",
            "10.1.5": "2023-07-11",
            "10.1.7": "2023-09-05",
            "10.2.0": "2023-11-07",
            "10.2.5": "2024-01-16",
            "10.2.6": "2024-03-19",
            "10.2.7": "2024-05-02",
        }
    },
    "The War Within": {
        "start": "2024-07-30",
        "end": "",
        "patch": {
            "11.0.0": "2024-07-30",
            "11.0.2": "2024-08-26",
            "11.0.5": "2024-10-22",
            "11.0.7": "2024-12-17",
            "11.1.0": "2025-02-25",
            "11.1.5": "2025-04-22",
            "11.1.7": "2025-06-17",
        }
    }
}

# Classic expansions and patches
classic_patches = {
    "Classic Era": {
        "start": "2017-11-03",
        "end": "",
        "patch": {
            "1.13.2": "2019-08-27"
        },
    },
    "Hardcore": {
        "start": "2023-08-24",
        "end": "",
        "patch": {
            "1.14.5": "2023-08-24",
        },
    },
    "Season of Discovery": {
        "start": "2023-11-30",
        "end": "",
        "patch": {
            "1.15.0": "2023-11-30",
        },
    },
    "The Burning Crusade Classic": {
        "start": "2021-06-01",
        "end": "2022-09-25",
        "patch": {
            "2.5.1": "2021-06-01",
        }
    },
    "Wrath of the Lich King Classic": {
        "start": "2022-09-26",
        "end": "2024-05-19",
        "patch": {
            "3.4.0": "2022-09-26",
        },
    },
    "Cataclysm Classic": {
        "start": "2024-05-20",
        "end": "",
        "patch": {
            "4.3.4": "2024-05-20",
        },
    },
}

# Mapping forums to expansions
forum_expansion_map = {
    "cataclysm classic discussion": [
        "The Burning Crusade Classic",
        "Wrath of the Lich King Classic",
        "Cataclysm Classic",
    ],
    "wow classic general discussion": ["Classic Era"],
    "season of discovery": ["Season of Discovery"],
    "hardcore": ["Hardcore Classic"],
}


class WoWPatchPipeline:
    """
    Pipeline for processing items related to World of Warcraft (WoW) forum data.

    This class is designed to handle WoW forum data items and enrich them with relevant
    attributes such as game version, expansion name, and patch version based on the
    forum name and post date. The pipeline determines whether the data corresponds to
    the 'classic' or 'retail' game version and performs processing accordingly.

    Attributes:
        forum_expansion_map (dict): Maps forum name patterns to their respective WoW expansions for classic versions.
        classic_patches (dict): Contains data about classic WoW expansions and their patch details, including start and
            end dates.
        retail_patches (dict): Contains data about retail WoW expansions and their patch details, including start and
            end dates.
    """

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        forum_name = adapter.get('forum_name', '')
        date_str = adapter.get('date_created')

        if not date_str:
            return item

        post_date = datetime.strptime(date_str[:10], '%Y-%m-%d')
        game_version = self.determine_game_version(forum_name)

        if game_version == "classic":
            expansions_to_consider = self.get_expansions_for_forum(forum_name)
            expansion, patch = self.find_classic_expansion_and_patch(post_date, expansions_to_consider)
        else:
            expansion, patch = self.find_expansion_and_patch(post_date, retail_patches)

        adapter['game_version'] = game_version
        adapter['expansion_name'] = expansion
        adapter['patch_version'] = patch

        return item

    def determine_game_version(self, forum_name: str) -> str:
        fn = forum_name.lower()
        return "classic" if "classic" in fn or "season of discovery" in fn else "retail"

    def get_expansions_for_forum(self, forum_name: str):
        fn = forum_name.lower()
        for name_pattern, expansions in forum_expansion_map.items():
            if name_pattern in fn:
                return expansions
        return []

    def parse_date(self, date_str: str) -> datetime:
        return datetime.strptime(date_str[:10], '%Y-%m-%d')

    def _select_patch_for_date(self, patches: dict, post_date: datetime) -> str:
        """
        Chooses the patch having the greatest release date that is less than or equal to post_date.
        Returns "Unknown" if none match.
        """
        best_patch = "Unknown"
        best_date = datetime.min
        for patch, patch_date_str in patches.items():
            release_date = self.parse_date(patch_date_str)
            if release_date <= post_date and release_date > best_date:
                best_date = release_date
                best_patch = patch
        return best_patch

    def find_classic_expansion_and_patch(self, post_date: datetime, expansions_list: list):
        for expansion in expansions_list:
            data = classic_patches.get(expansion)
            if not data:
                continue
            start = self.parse_date(data['start'])
            end = self.parse_date(data['end']) if data['end'] else datetime(2999, 1, 1)

            if start <= post_date <= end:
                patch = self._select_patch_for_date(data['patch'], post_date)
                return expansion, patch
        return "Unknown", "Unknown"

    def find_expansion_and_patch(self, post_date: datetime, expansions_dict: dict):
        for expansion, data in expansions_dict.items():
            start = self.parse_date(data['start'])
            end = self.parse_date(data['end']) if data['end'] else datetime(2999, 1, 1)
            if start <= post_date <= end:
                patch = self._select_patch_for_date(data['patch'], post_date)
                return expansion, patch
        return "Unknown", "Unknown"