from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class ServerForumFilterPipeline:
    """
    Filters out items based on their forum name, ensuring specific server forums
    are excluded during scraping.

    This pipeline is designed to work with spiders that define a list of
    'server_forum_names'. It checks if an item's 'forum_name' matches any of the
    defined server forum names and excludes such items from the output. The
    pipeline requires the spider to have a 'server_forum_names' attribute and
    validates this upon opening the spider.

    """

    @staticmethod
    def open_spider(spider):
        """
        Called when the spider is opened. This method can be used to validate
        that 'server_forum_names' exists on the spider.

        Args:
            spider (scrapy.Spider): The spider that is running.
        """
        if not hasattr(spider, 'server_forum_names') or not spider.server_forum_names:
            spider.logger.warning(
                "The spider is missing 'server_forum_names'. Filtering may not work as expected."
            )

    def process_item(self, item, spider):
        """
        Processes each item and determines whether it should be dropped based
        on its 'forum_name'.

        Args:
            item (dict): The item scraped by the spider.
            spider (scrapy.Spider): The spider that is running.

        Returns:
            dict: The item if it passes the filter.

        Raises:
            DropItem: If the 'forum_name' matches a server forum name.
        """
        adapter = ItemAdapter(item)
        forum_name = adapter.get('forum_name', '')

        if forum_name in spider.server_forum_names:
            raise DropItem(
                f"Discarding item from server forum: {forum_name}")
        return item