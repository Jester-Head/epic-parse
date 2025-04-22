from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class ServerForumFilterPipeline:
    """
    A Scrapy pipeline that filters out items from known 'server' (realm) forums.

    This pipeline drops items if their 'forum_name' matches any name in 
    the 'server_forum_names' attribute of the spider. The list of server forum
    names is set by the spider in the `parse_categories_json` method.

    Attributes:
        spider.server_forum_names (set): A set of server forum names to filter.
    """

    def open_spider(self, spider):
        """
        Called when the spider is opened. This method can be used to validate
        that 'server_forum_names' exists on the spider.

        Args:
            spider (scrapy.Spider): The spider that is running.
        """
        # Validate that the spider has 'server_forum_names' attribute
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
        

        # Drop the item if the forum name is in the list of server forums
        if forum_name in spider.server_forum_names:
            raise DropItem(
                f"Discarding item from server forum: {forum_name}")
        return item
