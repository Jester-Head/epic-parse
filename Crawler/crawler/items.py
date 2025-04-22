import scrapy


class WoWForumsItem(scrapy.Item):
    thread_id = scrapy.Field()
    post_id = scrapy.Field()
    url = scrapy.Field()
    # ------------------------------
    topic = scrapy.Field()
    forum_name = scrapy.Field()
    game_version = scrapy.Field()
    expansion_name = scrapy.Field()
    patch_version = scrapy.Field()
    # ------------------------------
    username = scrapy.Field()  # Full name with server
    name = scrapy.Field()      # Name without server
    server = scrapy.Field()    # Server extracted from username
    user_title = scrapy.Field()
    race = scrapy.Field()
    player_class = scrapy.Field()
    classic_andy = scrapy.Field()
    staff = scrapy.Field()
    # ------------------------------
    comment_text = scrapy.Field()
    quoted_text = scrapy.Field()
    quote_count = scrapy.Field()
    reply_count = scrapy.Field()
    likes = scrapy.Field()
    # ------------------------------
    date_created = scrapy.Field()
    date_updated = scrapy.Field()
