import logging
import logging.config
import pymongo

# Load your config variables for DB
from config import MONGO_URI, MONGO_DB, MONGO_COLL


class DatabaseConnection:
    def __init__(self, mongo_uri=MONGO_URI, mongo_db=MONGO_DB, mongo_coll=MONGO_COLL):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_coll = mongo_coll
        self.client = None
        self.db = None
        self.collection = None
        self.logger = logging.getLogger(__name__)

    def open_connection(self):
        """
        Establishes a connection to the MongoDB database and sets up the necessary indexes.
        """
        try:
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_db]
            self.collection = self.db[self.mongo_coll]
            self.collection.create_index(
                [("channelId", 1), ("likeCount", 1), ("updatedAt", 1)]
            )
            self.collection.create_index(
                [
                    ("channelId", pymongo.ASCENDING),
                    ("videoId", pymongo.ASCENDING),
                    ("updatedAt", pymongo.DESCENDING),
                ]
            )
            self.collection.create_index([("id", 1)], unique=True)
        except pymongo.errors.PyMongoError as e:
            self.logger.error(
                f"Error during database connection or index creation: {e}"
            )

    def get_most_recent_comment(self, channel_id, video_id):
        """
        Retrieve the most recent comment for a given channel and video.
        """
        most_recent_comment = self.collection.find_one(
            {"channelId": channel_id, "videoId": video_id},
            sort=[("updatedAt", pymongo.DESCENDING)]
        )
        return most_recent_comment

    def insert_comment(self, comment):
        """
        Inserts a single comment into the database collection (upsert).
        """
        try:
            if "id" not in comment:
                self.logger.warning(
                    "Comment is missing a unique 'id'; skipping insert.")
                return

            result = self.collection.update_one(
                # <-- Match on the actual comment ID
                {"id": comment["id"]},
                {"$setOnInsert": comment},
                upsert=True
            )
            if result.upserted_id is not None:
                self.logger.info(
                    f"Comment {comment['id']} inserted successfully.")
            else:
                self.logger.info(
                    f"No new comment inserted (duplicate or older). ID: {comment['id']}")

        except pymongo.errors.DuplicateKeyError:
            self.logger.warning("Duplicate comment not inserted.")
        except pymongo.errors.ConnectionFailure as e:
            self.logger.error(f"Database connection failed: {e}")
        except pymongo.errors.OperationFailure as e:
            self.logger.error(f"Operation failed: {e}")

    def insert_comments(self, comments):
        """
        Inserts multiple comments into the database collection (upsert each).
        """
        if not isinstance(comments, list):
            self.logger.warning("insert_comments expects a list of comments.")
            return

        for comment in comments:
            self.insert_comment(comment)

    def close_connection(self):
        if self.client:
            self.client.close()

    def __enter__(self):
        self.open_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close_connection()
        except Exception as e:
            self.logger.error(f"Failed to close database connection: {e}")
