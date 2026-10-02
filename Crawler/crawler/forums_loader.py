import re
from typing import Dict, List

import ftfy
from bs4 import BeautifulSoup
from itemloaders import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst, Join


class WoWForumsLoader(ItemLoader):
    """
    ItemLoader subclass designed to process and clean data scraped from World of Warcraft forums.

    The `WoWForumsLoader` class specializes in handling data extracted from World of Warcraft forums.
    It includes methods for processing text, removing quotes, and cleaning HTML input to produce
    structured and meaningful outputs. This class is equipped with data cleaning mechanisms to
    handle truncated tags, old post patterns, and whitespace issues, among others.

    Attributes:
        OLD_POST_PATTERN (Pattern): Regular expression to identify old post headers
            in quotes.
        TRUNCATED_TAG_PATTERN (Pattern): Regular expression to identify truncated tag
            leftovers in quotes.
        default_output_processor (TakeFirst): Default processor to fetch the first non-blank
            input value.

        username_in (MapCompose): Field processor to clean and strip usernames.
        server_in (MapCompose): Field processor to extract servers from usernames
            in the format "Name-Server".
        comment_text_in (MapCompose): Field processor to extract and clean comment text
            from HTML content.
        quoted_text_in (MapCompose): Field processor to extract quoted text
            from HTML input.
        quoted_text_out (Join): Field output processor to concatenate quoted text
            with a "|" separator.
        url_in (MapCompose): Field processor to clean URLs, removing trailing segments.
        comment_text_out (Join): Field output processor to concatenate comment text.
        classic_andy_in (MapCompose): Field processor to identify classic players by
            matching specific identifying patterns in input values.
    """
    # ------------------------------ constants ------------------------------ #
    OLD_POST_PATTERN = re.compile(
        r'\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2} [APM]{2}Posted by \w+\n*'
    )
    TRUNCATED_TAG_PATTERN = re.compile(
        r'<span class="truncated">.*?</span>', flags=re.DOTALL
    )
    # ------------------------------ defaults ------------------------------- #
    default_output_processor = TakeFirst()

    # --------------------------- helper methods ---------------------------- #
    @staticmethod
    def _extract_quotes_and_remaining(html: str) -> Dict[str, List[str] | str]:
        """
        Remove <aside class="quote"> and <blockquote> elements from the HTML while
        collecting their text content.

        Returns
        -------
        dict
            quoted_text : list[str]
            remaining_text : str
        """
        soup = BeautifulSoup(html, "html.parser")
        quotes: List[str] = []
        # Collect from <aside class="quote">
        for aside in soup.find_all("aside", class_="quote"):
            block = aside.find("blockquote")
            if block:
                quotes.append(block.get_text(strip=True))
            aside.decompose()
        # Collect from standalone <blockquote>
        for block in soup.find_all("blockquote"):
            quotes.append(block.get_text(strip=True))
            block.decompose()
        return {
            "quoted_text": quotes,
            "remaining_text": soup.get_text(strip=True),
        }

    # --------------------------- cleaning helpers -------------------------- #
    @classmethod
    def clean_quotes(cls, quotes: List[str]) -> List[str]:
        """
        Remove old style headers and truncated tag leftovers from quotes.
        """
        return [cls.TRUNCATED_TAG_PATTERN.sub("", cls.OLD_POST_PATTERN.sub("", q)).strip() for q in quotes]

    @staticmethod
    def clean_text(text: str) -> str:
        """
        Generic text cleanup: fix encoding, collapse whitespace, remove newlines.
        """
        if not text:
            return ""
        text = ftfy.fix_text(text)
        text = re.sub(r"\n", " ", text)
        return re.sub(r"\s+", " ", text.strip())

    # ------------------------- high-level pipeline ------------------------- #
    @classmethod
    def process_and_clean_quotes(cls, html: str) -> Dict[str, List[str] | str]:
        """
        Public API used by ItemLoader processors.
        Extracts, cleans quotes, and returns both cleaned quotes and comment body.
        """
        try:
            parsed = cls._extract_quotes_and_remaining(html)
            cleaned_quotes = cls.clean_quotes(parsed["quoted_text"])
            return {
                "quoted_text": cleaned_quotes,
                "comment_text": parsed["remaining_text"],
            }
        except (ValueError, AttributeError, TypeError) as e:
            # Log specific parsing errors and return fallback values
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Error parsing HTML content: {e}")
            return {"quoted_text": [], "comment_text": html}

    # -------------------------- extracted helper methods from inline lambdas --------- #
    @staticmethod
    def _extract_comment_text(html: str) -> str:
        """
        Extract and return the comment text from HTML by processing quotes.
        """
        return WoWForumsLoader.process_and_clean_quotes(html)["comment_text"]

    @staticmethod
    def _extract_quoted_text(html: str) -> List[str]:
        """
        Extract and return the quoted text from HTML.
        """
        return WoWForumsLoader.process_and_clean_quotes(html)["quoted_text"]

    # ------------------------------- misc ---------------------------------- #
    @staticmethod
    def extract_url(url: str) -> str:
        """Return URL without the trailing segment."""
        return "/".join(url.split("/")[:-1]).strip()

    @staticmethod
    def extract_server(username: str) -> str | None:
        """Extract 'Server' from 'Name-Server'."""
        if username and "-" in username:
            return username.split("-", 1)[1].strip()
        return None

    @classmethod
    def process_username(cls, username: str) -> str:
        """
        Potentially append server to username, depending on loader context.
        """
        username = username.strip()
        server = cls.extract_server(username)
        if loader_ctx := getattr(cls, "context", None):
            if loader_ctx.get("include_server") and server:
                return f"{username} ({server})"
        return username

    @staticmethod
    def _is_classic_player(value: str) -> bool:
        """Very naive classic identifier."""
        return bool(value and "t" in value.lower())

    # -------------------------- field processors --------------------------- #
    username_in = MapCompose(str.strip)
    server_in = MapCompose(extract_server)  # avoid instantiation
    comment_text_in = MapCompose(
        _extract_comment_text,
        clean_text,
    )
    quoted_text_in = MapCompose(_extract_quoted_text)
    quoted_text_out = Join("|")
    url_in = MapCompose(extract_url)
    comment_text_out = Join()
    classic_andy_in = MapCompose(_is_classic_player)