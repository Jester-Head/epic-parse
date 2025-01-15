# database_con.py

import logging
import pymongo
from datetime import datetime
from config import MONGO_URI, MONGO_DB, MONGO_COLL


class DatabaseConnection:
    def __init__(self, mongo_uri=MONGO_URI, mongo_db=MONGO_DB, mongo_coll=MONGO_COLL):
        """
        Initializes the DatabaseConnection with MongoDB URI, database name, and collection name.

        Args:
            mongo_uri (str): MongoDB connection URI.
            mongo_db (str): Name of the MongoDB database.
            mongo_coll (str): Name of the MongoDB collection for comments.
        """
        if not mongo_uri:
            raise ValueError("MONGO_URI must be provided")
        if not mongo_db:
            raise ValueError("MONGO_DB must be provided")
        if not mongo_coll:
            raise ValueError("MONGO_COLL must be provided")

        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_coll = mongo_coll
        self.client = None
        self.db = None
        self.collection = None
        self.progress_collection = None  # Collection for tracking progress
        self.logger = logging.getLogger(__name__)
        self.connect()

    def connect(self):
        """
        Establishes a connection to the MongoDB database and sets up the necessary indexes.
        """
        try:
            self.client = pymongo.MongoClient(
                self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[self.mongo_db]
            self.collection = self.db[self.mongo_coll]
            # Collection for progress tracking
            self.progress_collection = self.db['progress']

            # Define desired indexes with explicit names to avoid conflicts
            desired_indexes = [
                # Indexes for comments collection
                {
                    "collection": self.collection,
                    "keys": [("channel_id", pymongo.ASCENDING),
                             ("like_count", pymongo.ASCENDING),
                             ("updated_at", pymongo.ASCENDING)],
                    "unique": False,
                    "name": "channel_like_updated_idx"
                },
                {
                    "collection": self.collection,
                    "keys": [("channel_id", pymongo.ASCENDING),
                             ("video_id", pymongo.ASCENDING),
                             ("updated_at", pymongo.DESCENDING)],
                    "unique": False,
                    "name": "channel_video_updated_idx"
                },
                {
                    "collection": self.collection,
                    "keys": [("comment_id", pymongo.ASCENDING)],
                    "unique": True,
                    "name": "comment_id_unique_idx"
                },
                # Indexes for progress collection
                {
                    "collection": self.progress_collection,
                    "keys": [("video_id", pymongo.ASCENDING)],
                    "unique": True,
                    "name": "video_id_unique_idx",
                    "sparse": True  # Use sparse=True instead of partialFilterExpression
                }
            ]

            for index in desired_indexes:
                self.ensure_index(
                    collection=index["collection"],
                    keys=index["keys"],
                    unique=index["unique"],
                    name=index["name"],
                    # Pass sparse if specified
                    sparse=index.get("sparse", False)
                )

            self.logger.info(
                "Database connection established and indexes ensured.")
        except pymongo.errors.PyMongoError as e:
            self.logger.error(
                f"Error during database connection or index creation: {e}")
            raise e

    def ensure_index(self, collection, keys, unique, name, sparse=False):
        """
        Ensures that an index with the specified keys and options exists in the collection.

        If an index with the same name exists but with different options, it will be dropped and recreated.

        Args:
            collection (pymongo.collection.Collection): The MongoDB collection.
            keys (list of tuples): List of (field, direction) tuples.
            unique (bool): Whether the index should enforce uniqueness.
            name (str): The name of the index.
            sparse (bool, optional): Whether the index should be sparse.

        Returns:
            None
        """
        existing_indexes = collection.index_information()
        if name in existing_indexes:
            existing_index = existing_indexes[name]
            # Check if existing index has the same keys and unique option
            if (existing_index['key'] == keys) and (existing_index.get('unique', False) == unique):
                if sparse:
                    # MongoDB does not provide the 'sparse' option in index_information()
                    self.logger.warning(
                        f"Cannot verify 'sparse' option for index '{name}'. "
                        f"Ensure it matches the desired configuration."
                    )
                self.logger.debug(
                    f"Index '{name}' already exists with the desired configuration.")
                return
            else:
                # Drop the conflicting index
                try:
                    collection.drop_index(name)
                    self.logger.info(
                        f"Dropped existing index '{name}' due to configuration mismatch.")
                except pymongo.errors.PyMongoError as e:
                    self.logger.error(f"Failed to drop index '{name}': {e}")
                    raise e

        # Create the desired index
        try:
            if sparse:
                collection.create_index(
                    keys, unique=unique, name=name, sparse=True
                )
                self.logger.info(
                    f"Created index '{name}' on fields {keys} with unique={unique} and sparse={sparse}."
                )
            else:
                collection.create_index(keys, unique=unique, name=name)
                self.logger.info(
                    f"Created index '{name}' on fields {keys} with unique={unique}."
                )
        except pymongo.errors.PyMongoError as e:
            self.logger.error(f"Failed to create index '{name}': {e}")
            raise e

    def get_most_recent_comment(self, channel_id, video_id):
        """
        Retrieves the most recent comment for a given channel and video.

        Args:
            channel_id (str): The ID of the YouTube channel.
            video_id (str): The ID of the YouTube video.

        Returns:
            dict or None: The most recent comment document or None if not found.
        """
        try:
            most_recent_comment = self.collection.find_one(
                {"channel_id": channel_id, "video_id": video_id},
                sort=[("updated_at", pymongo.DESCENDING)]
            )
            return most_recent_comment
        except pymongo.errors.PyMongoError as e:
            self.logger.error(f"Error retrieving most recent comment: {e}")
            return None

    def insert_comment(self, comment):
        """
        Inserts a single comment into the database collection (upsert).

        Args:
            comment (dict): A dictionary containing comment data.

        Returns:
            None
        """
        try:
            if "comment_id" not in comment:
                self.logger.warning(
                    "Comment is missing a unique 'comment_id'; skipping insert.")
                return

            result = self.collection.update_one(
                {"comment_id": comment["comment_id"]},
                {"$setOnInsert": comment},
                upsert=True
            )
            if result.upserted_id is not None:
                self.logger.info(
                    f"Comment {comment['comment_id']} inserted successfully.")
            else:
                self.logger.debug(
                    f"No new comment inserted (duplicate or older). ID: {comment['comment_id']}")
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
                if "comment_id" not in comment:
                    self.logger.warning(
                        "Comment is missing a unique 'comment_id'; skipping insert.")
                    continue
                operations.append(
                    pymongo.UpdateOne(
                        {"comment_id": comment["comment_id"]},
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
        Retrieves the last saved page token for a specific video from the progress collection.

        Args:
            video_id (str): The ID of the YouTube video.

        Returns:
            str or None: The last page token if found; otherwise, None.
        """
        try:
            progress = self.progress_collection.find_one(
                {"video_id": video_id})
            return progress.get("last_page_token") if progress else None
        except pymongo.errors.PyMongoError as e:
            self.logger.error(
                f"Failed to retrieve progress for video ID {video_id}: {e}")
            return None

    def save_progress(self, video_id, page_token):
        """
        Saves the current page token for a specific video to the progress collection.

        Args:
            video_id (str): The ID of the YouTube video.
            page_token (str): The current page token.

        Returns:
            None
        """
        try:
            self.progress_collection.update_one(
                {"video_id": video_id},
                {
                    "$set": {
                        "last_page_token": page_token,
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
        """
        Enables usage of the DatabaseConnection class with the 'with' statement.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Ensures that the MongoDB connection is closed when exiting the 'with' block.
        """
        self.close_connection()
