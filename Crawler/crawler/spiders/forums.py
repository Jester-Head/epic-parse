import json
import logging.config

import scrapy
from utilities.config import LOGGING_CONFIG

from crawler.forums_loader import WoWForumsLoader
from crawler.items import WoWForumsItem

with open(LOGGING_CONFIG, 'rt') as f:
    config = json.load(f)

logging.config.dictConfig(config)


class WoWForumsSpider(scrapy.Spider):
    """
    Spider for scraping World of Warcraft forums hosted on Blizzard's US forums.

    This class is a Scrapy spider designed to crawl the Blizzard US forums for World of
    Warcraft. It extracts threads, posts, and metadata such as user details, forum names,
    and post content. The spider handles pagination seamlessly, identifies server-specific
    forums, and excludes certain forums from being scraped based on a deny list.

    Attributes:
        name (str): Name of the spider.
        allowed_domains (list): List of domains allowed for crawling.
        posts_per_request (int): Number of posts to fetch per API request.
        deny_forum_names (set): Set of forum names to exclude from scraping.
        server_forum_names (set): Set of server-specific forum names identified
            during the initial crawl.

    """
    name = "wow_forums_spider"
    allowed_domains = ["us.forums.blizzard.com"]

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

        self.server_forum_names = {
            cat["name"]
            for cat in categories
            if "is_realm" in cat.get("category_metadata", {})
        }
        self.logger.info(
            f"Server forum names identified: {self.server_forum_names}")

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
        thread_links = response.css('a.title::attr(href)').getall()
        for link in thread_links:
            full_url = response.urljoin(link)
            yield scrapy.Request(full_url, callback=self.parse_thread_html)

        next_link = response.css('a[rel="next"]::attr(href)').get()
        if next_link:
            yield scrapy.Request(response.urljoin(next_link), callback=self.parse_subforum)

    def parse_thread_html(self, response):
        """
        - Now we're on an actual thread page, e.g. /en/wow/t/foo/12345
        - Extract the numeric thread ID from the URL or the page.
        - Build the /posts.json endpoint and request it -> parse_api
        """

        parts = response.url.strip("/").split("/")
        thread_id = parts[-1]

        thread_id = thread_id.split("?")[0]

        api_url = f"https://us.forums.blizzard.com/en/wow/t/{thread_id}/posts.json"

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

            if forum_name in self.deny_forum_names:
                self.logger.info(
                    f"Skipping forum: {forum_name} (deny list match)")
                return

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
                        "include_server": True,
                        "default_server": "Unknown",
                    },
                )

                loader.add_value("thread_id", thread_id)
                loader.add_value("post_id", str(post.get("id")))
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

                comment_data = WoWForumsLoader.process_and_clean_quotes(
                    post.get("cooked", "")
                )
                loader.add_value("comment_text", comment_data["comment_text"])
                loader.add_value("quoted_text", comment_data["quoted_text"])
                loader.add_value("quote_count", len(
                    comment_data["quoted_text"]))

                loader.add_value("reply_count", post.get("reply_count"))
                loader.add_value(
                    "likes", self.extract_likes(
                        post.get("actions_summary", []))
                )
                loader.add_value("date_created", post.get("created_at"))
                loader.add_value("date_updated", post.get("updated_at"))

                yield loader.load_item()

            next_link = html_response.css('a[rel="next"]::attr(href)').get()
            if next_link:
                self.logger.info(f"Following next link: {next_link}")
                yield response.follow(next_link, callback=self.parse_thread_html)

        except Exception as e:
            self.logger.error(
                f"Error parsing API response for thread {response.meta['thread_id']}: {e}"
            )

    @staticmethod
    def extract_forum_name(response):
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

    @staticmethod
    def extract_likes(actions_summary):
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