import re
from bs4 import BeautifulSoup
import ftfy
from itemloaders import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst, Join


class WoWForumsLoader(ItemLoader):
    """
    Custom loader for processing forum data from World of Warcraft forums.
    """
    default_output_processor = TakeFirst()

    @staticmethod
    def extract_quotes(value):
        """
        Extract quotes from the HTML content and leave the remaining text intact.

        Args:
            value (str): Raw HTML content of the forum post.

        Returns:
            dict: A dictionary with 'quoted_text' (list of quotes) and 
                  'remaining_text' (str of non-quoted content).
        """
        soup = BeautifulSoup(value, 'html.parser')
        quotes = []

        # Process quotes inside <aside> elements
        for aside in soup.find_all('aside', class_='quote'):
            blockquote = aside.find('blockquote')
            if blockquote:
                quotes.append(blockquote.get_text().strip())
            aside.decompose()

        # Process standalone <blockquote> tags
        for blockquote in soup.find_all('blockquote'):
            quotes.append(blockquote.get_text().strip())
            blockquote.decompose()

        # Return extracted quotes and remaining content
        return {
            'quoted_text': quotes,
            'remaining_text': soup.get_text().strip()
        }

    @staticmethod
    def clean_quotes(quotes):
        """
        Clean extracted quotes by removing unwanted patterns and formatting.

        Args:
            quotes (list): List of quotes to be cleaned.

        Returns:
            list: Cleaned quotes.
        """
        cleaned_quotes = []
        for quote in quotes:
            # Remove old post formatting (e.g., "10/28/2018 09:08 PMPosted by Snowfox")
            quote = re.sub(
                r'\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2} [APM]{2}Posted by \w+\n*',
                '', quote
            )
            # Remove truncated tags (e.g., <span class="truncated">...</span>)
            quote = re.sub(
                r'<span class="truncated">.*?</span>', '', quote, flags=re.DOTALL
            )
            cleaned_quotes.append(quote.strip())
        return cleaned_quotes

    @staticmethod
    def process_and_clean_quotes(value):
        """
        Process raw HTML content to extract and clean quotes while preserving the remaining text.

        Args:
            value (str): Raw HTML content.

        Returns:
            dict: Dictionary with 'quoted_text' (list of cleaned quotes) and 
                  'comment_text' (remaining text after quotes are removed).
        """
        try:
            soup = BeautifulSoup(value, 'html.parser')
            quotes = []

            # Extract quotes from <aside> elements
            for aside in soup.find_all('aside', class_='quote'):
                blockquote = aside.find('blockquote')
                if blockquote:
                    quotes.append(blockquote.get_text().strip())
                aside.decompose()

            # Extract quotes from standalone <blockquote> tags
            for blockquote in soup.find_all('blockquote'):
                quotes.append(blockquote.get_text().strip())
                blockquote.decompose()

            # Clean extracted quotes
            cleaned_quotes = WoWForumsLoader.clean_quotes(quotes)

            return {
                'quoted_text': cleaned_quotes,
                'comment_text': soup.get_text().strip()
            }

        except (ValueError, AttributeError, TypeError) as e:
            # Log specific parsing errors and return fallback values
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Error parsing HTML content: {e}")
            return {'quoted_text': [], 'comment_text': value}

    @staticmethod
    def clean_text(value):
        """
        Clean general text content by fixing encoding issues and removing excessive whitespace.

        Args:
            value (str): Text to be cleaned.

        Returns:
            str: Cleaned text.
        """
        if value:
            value = ftfy.fix_text(value)  # Fix encoding issues
            value = re.sub(r'\n', ' ', value)  # Replace newlines with spaces
            value = re.sub(r'\s+', ' ', value.strip())  # Normalize whitespace
        return value

    @staticmethod
    def extract_url(value):
        """
        Extract the base URL by removing the trailing part after the last slash.

        Args:
            value (str): Full URL.

        Returns:
            str: Extracted base URL.
        """
        return '/'.join(value.split('/')[:-1]).strip()

    def extract_server(self, value):
        """
        Extract the server name from the username.

        Args:
            value (str): Username in the format "Name-Server".

        Returns:
            str: Server name or a default server if not present.
        """
        if value and '-' in value:
            return value.split('-', 1)[1].strip()
        return self.context.get('default_server', None)

    def process_username(self, value):
        """
        Process username to optionally include server information.

        Args:
            value (str): Raw username.

        Returns:
            str: Processed username with or without server.
        """
        username = value.strip()
        server = self.extract_server(username)
        if 'include_server' in self.context and server:
            return f"{username} ({server})"
        return username

    def is_classic_player(value):
        """
        Determine if the player is from Classic WoW based on their user details.

        Args:
            value (str): Username or text indicator.

        Returns:
            bool: True if Classic, False otherwise.
        """
        return bool(value and 't' in value.lower())

    # Field-specific processors
    username_in = MapCompose(str.strip)
    server_in = MapCompose(lambda x: WoWForumsLoader().extract_server(x))
    comment_text_in = MapCompose(
        lambda x: WoWForumsLoader.process_and_clean_quotes(x)['comment_text'],
        clean_text
    )
    classic_andy_in = MapCompose(is_classic_player)
    quoted_text_in = MapCompose(lambda x: x)  # Raw values for quoted text
    quoted_text_out = Join('|')  # Combine quoted texts into a single string
    url_in = MapCompose(lambda x: WoWForumsLoader.extract_url(x))
    comment_text_out = Join()  # Combine comment text into a single string
