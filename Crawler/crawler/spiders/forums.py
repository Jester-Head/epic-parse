import json
import logging
from logging.handlers import RotatingFileHandler
import scrapy
import logging.config

from crawler.forums_loader import WoWForumsLoader
from crawler.items import WoWForumsItem
from utilities.config import LOGGING_CONFIG

# Load logging configuration
with open(LOGGING_CONFIG, 'rt') as f:
    config = json.load(f)

logging.config.dictConfig(config)

class WoWForumsSpider(scrapy.Spider):
    name = "wow_forums_spider"
    allowed_domains = ["us.forums.blizzard.com"]

    posts_per_request = 20
    posts_per_request = 20

    deny_forum_names = {
        "Off-Topic",
        "Support",
        "Recruitment",
        "UI and Macro",
        "WoW Classic New Guild Listings",
        "Classic Connections 2004-2010 - Find People Here"}

    server_forum_names = set()

    def start_requests(self):
        """
        First request the categories.json endpoint to identify 'server' forums
        and store them in self.server_forum_names.
        Then yield requests for the subforum pages where we want to crawl threads.
        """
        categories_url = "https://us.forums.blizzard.com/en/wow/categories.json"
        yield scrapy.Request(categories_url, callback=self.parse_categories_json)

    def parse_categories_json(self, response):
        """
        Parse the categories.json file to identify server-specific forums.
        Then start crawling whichever forum pages you want (subforums, top, etc.).
        """
        data = json.loads(response.text)
        categories = data.get("category_list", {}).get("categories", [])

        # Extract server forum names based on a "is_realm" flag in category_metadata
        self.server_forum_names = {
            cat["name"]
            for cat in categories
            if "is_realm" in cat.get("category_metadata", {})
        }
        self.logger.info(
            f"Server forum names identified: {self.server_forum_names}")

        # Now yield requests to your subforum pages (or the main page).
        # Example: the subforum URLs you provided earlier:
        subforum_urls = [
            "https://us.forums.blizzard.com/en/wow/latest?ascending=false&order=posts",
            "https://us.forums.blizzard.com/en/wow/c/in-development/23/l/latest",
            "https://us.forums.blizzard.com/en/wow/c/community/170?ascending=true&order=activity",
            "https://us.forums.blizzard.com/en/wow/c/gameplay/36?ascending=true&order=activity",
            "https://us.forums.blizzard.com/en/wow/c/wow-classic/197?ascending=true&order=activity",
            "https://us.forums.blizzard.com/en/wow/c/lore/47?ascending=true&order=activity",
            "https://us.forums.blizzard.com/en/wow/c/classes/174?ascending=true&order=activity",
            "https://us.forums.blizzard.com/en/wow/c/pvp/20?ascending=true&order=activity",
        ]

        for url in subforum_urls:
            yield scrapy.Request(url=url, callback=self.parse_subforum)

    def parse_subforum(self, response):
        """
        From a category/subforum page, identify thread links and follow them.
        Also handle pagination if needed.
        """
        # Thread links typically look like /en/wow/t/<slug>/<number>
        thread_links = response.css('a.title::attr(href)').getall()
        for link in thread_links:
            full_url = response.urljoin(link)
            yield scrapy.Request(full_url, callback=self.parse_thread_html)

        # If there's a "next" page in the subforum, follow it
        next_link = response.css('a[rel="next"]::attr(href)').get()
        if next_link:
            yield scrapy.Request(response.urljoin(next_link), callback=self.parse_subforum)

    def parse_thread_html(self, response):
        """
        - Now we're on an actual thread page, e.g. /en/wow/t/foo/12345
        - Extract the numeric thread ID from the URL or the page.
        - Build the /posts.json endpoint and request it -> parse_api
        """
        # Example URL: https://us.forums.blizzard.com/en/wow/t/foo/12345
        # Let's split on '/' and take the last segment
        parts = response.url.strip("/").split("/")
        # The last part is usually the numeric ID
        thread_id = parts[-1]

        # Some threads can end with '?something=xyz' so let's remove query params if needed
        thread_id = thread_id.split("?")[0]

        # Build the API URL
        api_url = f"https://us.forums.blizzard.com/en/wow/t/{thread_id}/posts.json"

        # We'll pass along the original HTML response if you want to extract the "forum name" from it
        yield scrapy.Request(
            url=api_url,
            callback=self.parse_api,
            meta={
                "thread_id": thread_id,
                "html_response": response,
                "start": 0  # for pagination
            }
        )

    def parse_api(self, response):
        """
        Parse the JSON API response for thread posts and process each post.

        Args:
            response (scrapy.http.Response): Response object for a posts.json API request.
        """
        try:
            data = json.loads(response.text)
            thread_id = response.meta["thread_id"]
            html_response = response.meta.get("html_response", None)
            start = response.meta.get("start", 0)

            forum_name = data.get("forum_name",
                                  self.extract_forum_name(html_response)
                                  if html_response
                                  else "Unknown",
                                  )

            # 3) Skip if forum_name is in the deny list
            if forum_name in self.deny_forum_names:
                self.logger.info(
                    f"Skipping forum: {forum_name} (deny list match)")
                return

            # Extract posts from the API response
            posts = data.get("post_stream", {}).get("posts", [])
            if not posts:
                self.logger.info(
                    f"No more posts found for thread {thread_id}.")
                return

            for post in posts:
                loader = WoWForumsLoader(
                    item=WoWForumsItem(),
                    selector=None,
                    context={
                        "include_server": True,  # Include server in username if available
                        "default_server": "Unknown",  # Default server name if not provided
                    },
                )

                # Add metadata and user details
                loader.add_value("thread_id", thread_id)
                loader.add_value("post_id", post.get("id"))
                loader.add_value("url", response.url)
                loader.add_value(
                    "forum_name",
                    self.extract_forum_name(html_response)
                    if html_response
                    else "Unknown",
                )
                loader.add_value("username", post.get("username", ""))
                loader.add_value("user_title", post.get("user_title"))
                loader.add_value("race", post.get(
                    "user_custom_fields", {}).get("race"))
                loader.add_value(
                    "player_class", post.get(
                        "user_custom_fields", {}).get("class")
                )
                loader.add_value("classic_andy", post.get("classic", False))
                loader.add_value("staff", post.get("staff", False))

                # Process and clean comment and quoted text
                comment_data = WoWForumsLoader.process_and_clean_quotes(
                    post.get("cooked", "")
                )
                loader.add_value("comment_text", comment_data["comment_text"])
                loader.add_value("quoted_text", comment_data["quoted_text"])
                loader.add_value("quote_count", len(
                    comment_data["quoted_text"]))

                # Add post statistics
                loader.add_value("reply_count", post.get("reply_count"))
                loader.add_value(
                    "likes", self.extract_likes(
                        post.get("actions_summary", []))
                )
                loader.add_value("date_created", post.get("created_at"))
                loader.add_value("date_updated", post.get("updated_at"))

                # Yield the processed item
                yield loader.load_item()

            next_link = html_response.css('a[rel="next"]::attr(href)').get()
            if next_link:
                self.logger.info(f"Following next link: {next_link}")
                yield response.follow(next_link, callback=self.parse_thread_html)

        except Exception as e:
            self.logger.error(
                f"Error parsing API response for thread {response.meta['thread_id']}: {e}"
            )

    def extract_forum_name(self, response):
        """
        Extract the forum name from the page title or fallback options.

        Args:
            response (scrapy.http.Response): HTML response from a thread page.

        Returns:
            str: Extracted forum name or 'Unknown' if not found.
        """
        forum_name = response.xpath(
            '//*[@id="topic-title"]/div/span[2]/a/span[2]/span/text()'
        ).get() or response.xpath(
            '//*[@id="topic-title"]/div/span[1]/a/span[2]/span/text()'
        ).get()
        return forum_name or "Unknown"

    def extract_likes(self, actions_summary):
        """
        Extract the count of likes from the post's action summary.

        Args:
            actions_summary (list): List of action dictionaries for a post.

        Returns:
            int: Number of likes, or 0 if no likes are found.
        """
        for action in actions_summary or []:
            if action.get("id") == 2:
                return action.get("count", 0)
        return 0
