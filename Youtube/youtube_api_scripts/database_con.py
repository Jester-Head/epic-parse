import logging
import logging.config
import pymongo
from datetime import datetime
# Load your config variables for DB
from config import MONGO_URI, MONGO_DB, MONGO_COLL


class DatabaseConnection:
    def __init__(self, mongo_uri=MONGO_URI, mongo_db=MONGO_DB, mongo_coll=MONGO_COLL):
        if mongo_uri is None:
            raise ValueError("MONGO_URI must be provided")
        if mongo_db is None:
            raise ValueError("MONGO_DB must be provided")
        if mongo_coll is None:
            raise ValueError("MONGO_COLL must be provided")
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_coll = mongo_coll
        self.client = None
        self.db = None
        self.collection = None
        self.progress_collection = None  # New collection for progress
        self.logger = logging.getLogger(__name__)
        self.client = pymongo.MongoClient(
            self.mongo_uri, serverSelectionTimeoutMS=5000)

    def open_connection(self):
        """
        Establishes a connection to the MongoDB database and sets up the necessary indexes.
        """
        try:
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_db]
            self.collection = self.db[self.mongo_coll]
            # Set up the progress collection
            self.progress_collection = self.db['progress']

            # Ensure indexes on comments collection
            # Index on channelId, likeCount, and updatedAt to optimize queries for comments
            # sorted by like count within a channel and updated time.
            self.collection.create_index(
                [("snippet.channelId", pymongo.ASCENDING),
                 ("snippet.likeCount", pymongo.ASCENDING),
                 ("snippet.updatedAt", pymongo.ASCENDING)]
            )
            self.collection.create_index(
                [
                    ("snippet.channelId", pymongo.ASCENDING),
                    ("snippet.videoId", pymongo.ASCENDING),
                    ("snippet.updatedAt", pymongo.DESCENDING),
                ]
            )
            self.collection.create_index(
                [("id", pymongo.ASCENDING)], unique=True)
            # Indexing videoId in progress_collection to ensure unique entries for each video
            self.progress_collection.create_index(
                [("videoId", pymongo.ASCENDING)], unique=True
            )
            self.logger.info(
                "Database connection established and indexes created.")
        except pymongo.errors.PyMongoError as e:
            self.logger.error(
                f"Error during database connection or index creation: {e}"
            )

    def get_most_recent_comment(self, channel_id, video_id):
        """
        Retrieve the most recent comment for a given channel and video.
        """
        try:
            most_recent_comment = self.collection.find_one(
                {"snippet.channelId": channel_id, "snippet.videoId": video_id},
                sort=[("snippet.updatedAt", pymongo.DESCENDING)]
            )
            return most_recent_comment
        except pymongo.errors.PyMongoError as e:
            self.logger.error(f"Error retrieving most recent comment: {e}")
            return None

    def insert_comment(self, comment):
        """
        Inserts a single comment into the database collection (upsert).
        """
        try:
            if "id" not in comment:
                self.logger.warning(
                    "Comment is missing a unique 'id'; skipping insert."
                )
                return

            result = self.collection.update_one(
                {"id": comment["id"]},
                {"$setOnInsert": comment},
                upsert=True
            )
            if result.upserted_id is not None:
                self.logger.info(
                    f"Comment {comment['id']} inserted successfully."
                )
            else:
                self.logger.debug(
                    f"No new comment inserted (duplicate or older). ID: {comment['id']}"
                )
        except pymongo.errors.DuplicateKeyError:
            self.logger.warning("Duplicate comment not inserted.")
        except pymongo.errors.PyMongoError as e:
            self.logger.error(f"Database operation failed: {e}")

    def insert_comments(self, comments):
        """
        Inserts multiple comments into the database collection (bulk upsert).

        Args:
            comments (list): A list of comment dictionaries.

        Returns:
            None
        """
        if not isinstance(comments, list):
            self.logger.warning("insert_comments expects a list of comments.")
            return

        try:
            operations = []
            for comment in comments:
                if "id" not in comment:
                    self.logger.warning(
                        "Comment is missing a unique 'id'; skipping insert.")
                    continue
                operations.append(
                    pymongo.UpdateOne(
                        {"id": comment["id"]},
                        {"$setOnInsert": comment},
                        upsert=True
                    )
                )
            if operations:
                result = self.collection.bulk_write(operations, ordered=False)
                self.logger.info(
                    f"Inserted {result.upserted_count} new comments.")
        except pymongo.errors.BulkWriteError as bwe:
            self.logger.error(f"Bulk write error: {bwe.details}")
        except pymongo.errors.PyMongoError as e:
            self.logger.error(f"Failed to insert comments: {e}")

    def get_progress(self, video_id):
        """
        Retrieve the last saved page token for a video from the progress collection.

        Args:
            video_id (str): The ID of the YouTube video.

        Returns:
            str or None: The last page token or None if not found.
        """
        try:
            progress = self.progress_collection.find_one({"videoId": video_id})
            return progress.get("lastPageToken", None) if progress else None
        except pymongo.errors.PyMongoError as e:
            self.logger.error(
                f"Failed to retrieve progress for video ID {video_id}: {e}")
            return None

    def save_progress(self, video_id, page_token):
        """
        Save the current page token for a video to the progress collection.

        Args:
            video_id (str): The ID of the YouTube video.
            page_token (str): The current page token.
        """
        try:
            self.progress_collection.update_one(
                {"videoId": video_id},
                {
                    "$set": {
                        "lastPageToken": page_token,
                        "timestamp": datetime.utcnow()
                    }
                },
                upsert=True
            )
            self.logger.info(
                f"Progress saved for video ID {video_id}. Page Token: {page_token}")
        except pymongo.errors.PyMongoError as e:
            self.logger.error(
                f"Failed to save progress for video ID {video_id}: {e}")

    def close_connection(self):
        """
        Closes the MongoDB connection.
        """
        if self.client:
            try:
                self.client.close()
                self.logger.info("Database connection closed.")
            except pymongo.errors.PyMongoError as e:
                self.logger.error(f"Error closing database connection: {e}")

    def __enter__(self):
        self.open_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()
