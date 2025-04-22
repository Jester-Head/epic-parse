import pymongo
from itemadapter import ItemAdapter
from pymongo import errors


class DatabasePipeline:
    def __init__(self, mongo_uri, mongo_db, mongo_coll):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_coll = mongo_coll

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE", "default_db"),
            mongo_coll=crawler.settings.get(
                "MONGO_COLL_FORUMS", "default_collection"),
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.collection = self.db[self.mongo_coll]

        # Create a unique compound index for identifying unique comments.
        try:
            self.collection.create_index([
                ("thread_id", 1),
                ("post_id", 1),
            ], unique=True)
        except errors.OperationFailure as e:
            spider.logger.error(f"Error creating index: {e}")

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        """
        Process each item, either inserting a new post or updating an existing one
        if the number of likes or replies has changed.
        """
        item_dict = ItemAdapter(item).asdict()
        # spider.logger.info(f"Attempting DB insert for item: {item}")

        query = {
            "thread_id": item_dict.get("thread_id"),
            "post_id": item_dict.get("post_id"),
        }

        update_data = {
            "likes": item_dict.get("likes"),
            "reply_count": item_dict.get("reply_count"),
            "date_updated": item_dict.get("date_updated"),
        }

        try:
            # Check if the document already exists
            existing_document = self.collection.find_one(query)
            if existing_document:
                # Check if the likes or reply_count have changed
                if (
                    existing_document.get("likes") != item_dict.get("likes")
                    or existing_document.get("reply_count") != item_dict.get("reply_count")
                ):
                    # Update the document if the relevant fields have changed
                    self.collection.update_one(query, {"$set": update_data})
                    spider.logger.info(
                        f"Updated post: Thread ID {query['thread_id']}, Post ID {query['post_id']}")
            else:
                # Insert the document if it doesn't exist
                self.collection.insert_one(item_dict)
                spider.logger.info(
                    f"DB insert SUCCESS for Thread ID {query['thread_id']}, Post ID {query['post_id']}")
        except Exception as e:
            spider.logger.error(f"Error processing item: {e}")

        return item
