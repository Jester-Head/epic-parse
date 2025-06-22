import logging
from datetime import datetime, timezone

from pymongo import ASCENDING, DESCENDING, MongoClient, errors
from pymongo.operations import UpdateOne

from config import MONGO_COLL, MONGO_DB, MONGO_URI


class DatabaseConnection:
    """
    Provides functionality to manage a MongoDB connection, collections, and indexing.

    This class encapsulates the logic for connecting to a MongoDB instance, creating
    required indexes, and performing various database operations such as inserting
    comments and managing progress tokens. It is designed to streamline interactions
    with the database by providing methods for commonly needed operations while ensuring
    data integrity and logging errors.

    Attributes:
        mongo_uri (str): The connection URI for the MongoDB instance.
        mongo_db (str): The name of the MongoDB database to connect to.
        mongo_coll (str): The name of the primary collection within the MongoDB database.
        client (MongoClient): The MongoDB client instance used for the connection.
        db (Database): The MongoDB database instance.
        collection (Collection): The primary collection for storing comment data.
        progress_collection (Collection): The collection for tracking progress data.
        logger (Logger): The logger used for recording information, warnings, and errors.
    """

    def __init__(self, mongo_uri=MONGO_URI, mongo_db=MONGO_DB, mongo_coll=MONGO_COLL):
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
        self.progress_collection = None

        self.logger = logging.getLogger(__name__)
        self.connect()

    def connect(self):
        """Open the connection and create required indexes."""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[self.mongo_db]
            self.collection = self.db[self.mongo_coll]
            self.progress_collection = self.db["progress"]

            self.progress_collection.create_index(
                "timestamp",
                name="progress_ttl_idx",
                expireAfterSeconds=60 * 60 * 24 * 30  # 30 days
            )

            desired_indexes = [
                {
                    "collection": self.collection,
                    "keys": [
                        ("channel_id", ASCENDING),
                        ("like_count", ASCENDING),
                        ("updated_at", ASCENDING),
                    ],
                    "unique": False,
                    "name": "channel_like_updated_idx",
                },
                {
                    "collection": self.collection,
                    "keys": [
                        ("channel_id", ASCENDING),
                        ("video_id", ASCENDING),
                        ("updated_at", DESCENDING),
                    ],
                    "unique": False,
                    "name": "channel_video_updated_idx",
                },
                {
                    "collection": self.collection,
                    "keys": [("comment_id", ASCENDING)],
                    "unique": True,
                    "name": "comment_id_unique_idx",
                },
            ]

            for idx in desired_indexes:
                self.ensure_index(
                    collection=idx["collection"],
                    keys=idx["keys"],
                    unique=idx["unique"],
                    name=idx["name"],
                    sparse=idx.get("sparse", False),
                )

            self.logger.info("Database connection established and indexes ensured.")
        except errors.PyMongoError as exc:
            self.logger.error("DB connection/index error: %s", exc)
            raise exc

    #  Index management
    def ensure_index(self, collection, keys, unique, name, sparse=False):
        existing = collection.index_information()
        if name in existing:
            idx = existing[name]
            if idx["key"] == keys and idx.get("unique", False) == unique:
                return
            collection.drop_index(name)
            self.logger.info("Dropped index %s for re-creation", name)

        collection.create_index(keys, unique=unique, name=name, sparse=sparse)
        self.logger.info("Created index %s", name)

    #  Comment helpers
    def get_most_recent_comment(self, channel_id, video_id):
        try:
            return self.collection.find_one(
                {"channel_id": channel_id, "video_id": video_id},
                sort=[("updated_at", DESCENDING)],
            )
        except errors.PyMongoError as exc:
            self.logger.error("Error retrieving most recent comment: %s", exc)
            return None

    def insert_comments(self, comments: list):
        """
        Bulk upsert.  Each document now writes `video_publish_date`.
        Robust against missing keys, oversize batches, and logs full result stats.
        """
        if not isinstance(comments, list):
            self.logger.warning("insert_comments expects a list.")
            return

        # Build operations in batches of 1 000 to stay under Mongo limits
        batch, total_upserted, total_matched, total_modified = [], 0, 0, 0
        required_keys = {
            "comment_id", "video_id", "video_title", "channel_id",
            "channel_name", "author", "text", "like_count",
            "published_at", "updated_at"
        }

        def flush(current_batch):
            nonlocal total_upserted, total_matched, total_modified
            if not current_batch:
                return
            try:
                res = self.collection.bulk_write(current_batch, ordered=False)
                total_upserted += res.upserted_count
                total_matched += res.matched_count
                total_modified += res.modified_count
            except errors.BulkWriteError as bwe:
                self.logger.error("Bulk write error: %s", bwe.details)
            except errors.PyMongoError as exc:
                self.logger.error("insert_comments failed: %s", exc)

        for cm in comments:
            if not required_keys.issubset(cm):
                self.logger.warning("Skipping malformed comment %s", cm.get("comment_id"))
                continue

            batch.append(
                UpdateOne(
                    {"comment_id": cm["comment_id"]},
                    {"$set": {
                        "video_id": cm["video_id"],
                        "video_title": cm["video_title"],
                        "channel_id": cm["channel_id"],
                        "channel_name": cm["channel_name"],
                        "video_publish_date": cm.get("video_publish_date"),
                        "author": cm["author"],
                        "author_channel_id": cm.get("author_channel_id"),
                        "text": cm["text"],
                        "like_count": cm["like_count"],
                        "published_at": cm["published_at"],
                        "updated_at": cm["updated_at"],
                    }},
                    upsert=True,
                )
            )

            if len(batch) >= 1000:
                flush(batch)
                batch = []

        flush(batch)

        self.logger.info(
            "Bulk result – upserted:%d matched:%d modified:%d",
            total_upserted, total_matched, total_modified
        )

    #  Progress helpers (video- or channel-level)

    def get_progress(self, key: str):
        """
        Return the stored page token for *key* (_id in the progress collection).
        • key can be a video ID (“dQw4w9…”) or a channel sentinel (“chan::UCxyz”).
        • Returns None if the row doesn’t exist OR if the token is the sentinel.
        """
        try:
            row = self.progress_collection.find_one({"_id": key})
            return row.get("last_page_token") if row else None
        except errors.PyMongoError as exc:
            self.logger.error("get_progress failed: %s", exc)
            return None

    def progress_exists(self, key: str) -> bool:
        """True if a progress document with *_id == key* already exists."""
        try:
            return self.progress_collection.count_documents({"_id": key}, limit=1) == 1
        except errors.PyMongoError as exc:
            self.logger.error("progress_exists failed: %s", exc)
            return False

    def save_progress(self, key: str, page_token):
        """
        Upsert a progress row.  Use *page_token=None* as the “all caught up” sentinel.
        """
        try:
            self.progress_collection.update_one(
                {"_id": key},
                {"$set": {
                    "last_page_token": page_token,
                    "timestamp": datetime.now(timezone.utc),
                }},
                upsert=True,
            )
        except errors.PyMongoError as exc:
            self.logger.error("save_progress failed: %s", exc)

    #  Context-manager plumbing

    def close_connection(self):
        if self.client:
            try:
                self.client.close()
                self.logger.info("Mongo connection closed.")
            except errors.PyMongoError as exc:
                self.logger.error("Error closing Mongo connection: %s", exc)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()
