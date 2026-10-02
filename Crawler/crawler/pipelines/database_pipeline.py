import pymongo
from itemadapter import ItemAdapter
from pymongo import errors


class DatabasePipeline:
    """
    Manages database operations for processing items in a web scraping pipeline.

    This class facilitates interaction with a MongoDB database by handling item
    inserts and updates. Primarily used in conjunction with a web scraping
    framework, this class ensures accurate data storage and prevents duplication by
    leveraging unique indexes.

    Attributes:
        client (pymongo.MongoClient): MongoDB client for database connection.
        db (pymongo.database.Database): MongoDB database instance.
        collection (pymongo.collection.Collection): MongoDB collection instance.
    """

    def __init__(self, mongo_uri, mongo_db, mongo_collection):
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[mongo_db]
        self.collection = self.db[mongo_collection]

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE", "default_db"),
            mongo_collection=crawler.settings.get("MONGO_COLL_FORUMS", "default_collection"),
        )

    def open_spider(self, spider):
        try:
            self.collection.create_index(
                [("thread_id", 1), ("post_id", 1)],
                unique=True,
            )
        except errors.OperationFailure as e:
            spider.logger.error(f"Error creating index: {e}")

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        """
        Process each item by either inserting a new post or updating an existing one
        if the number of likes or replies has changed.
        """
        item_dict = ItemAdapter(item).asdict()
        query = {
            "thread_id": item_dict.get("thread_id"),
            "post_id": item_dict.get("post_id"),
        }
        updated_fields = {
            "likes": item_dict.get("likes"),
            "reply_count": item_dict.get("reply_count"),
            "date_updated": item_dict.get("date_updated"),
        }
        try:
            existing_document = self.collection.find_one(query)
            if existing_document:
                if self._has_relevant_changes(existing_document, updated_fields):
                    self._update_document(query, updated_fields, spider)
            else:
                self.collection.insert_one(item_dict)
                spider.logger.info(
                    f"DB insert SUCCESS for Thread ID {query['thread_id']}, Post ID {query['post_id']}"
                )
        except Exception as e:
            spider.logger.error(f"Error processing item: {e}")
        return item

    def _has_relevant_changes(self, existing_document: dict, updated_fields: dict) -> bool:
        return (
                existing_document.get("likes") != updated_fields.get("likes")
                or existing_document.get("reply_count") != updated_fields.get("reply_count")
        )

    def _update_document(self, query: dict, updated_fields: dict, spider) -> None:
        self.collection.update_one(query, {"$set": updated_fields})
        spider.logger.info(
            f"Updated post: Thread ID {query['thread_id']}, Post ID {query['post_id']}"
        )