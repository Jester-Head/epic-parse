# YouTube Comment Scraper

## Overview

A YouTube comment scraper that fetches comments from YouTube channels and playlists with advanced filtering,
caching.

## Structure

### Core Modules

#### `utils/`

- **logging_setup.py**: Configures logging system
- **cache.py**: LRU cache implementation for video/channel metadata

#### `api/`

- **youtube_client.py**: YouTube API client with quota management and retries
- **metadata.py**: Video and channel metadata fetching
- **comments.py**: Comment fetching and processing
- **channels.py**: Channel operations (verification, stats, etc.)
- **playlists.py**: Playlist operations and video discovery

#### `core/`

- **processor.py**: Main orchestration logic

#### `cli/`

- **arguments.py**: Command-line argument parsing
- **channel_filter.py**: Channel filtering logic

## Features

### API Management

- **Quota Handling**: Automatic API key rotation when quotas are exhausted (**ROTATION NOT RECOMMENDED**)
- **Retry Logic**: Exponential backoff for transient errors
- **Rate Limiting**: Respect API rate limits

### Caching

- **LRU Cache**: Efficient memory usage with automatic eviction
- **Persistent Storage**: Cache survives across runs
- **Metadata Caching**: Avoid redundant API calls for video/channel info

### Resume Capability

- **Progress Tracking**: Resume from where processing left off
- **Database Integration**: Persistent progress storage
- **Incremental Updates**: Only fetch new comments

## Usage

```bash
# Basic usage
python main.py

# Filter by channel types
python main.py --types news,lore

# Verify channel still exists and is active
python main.py --verify-channels

# Process specific channels
python main.py --channels channel1,channel2

# Exclude inactive channels
python main.py --max-inactive-days 30
```

## Configuration

Create a `config.py` file with:

```python
API_KEYS = ["your_api_key_1", "your_api_key_2"]  # This works with one key. Add more keys at your own risk. 
CHANNELS = {
    "channel_name": {
        "channel_id": "UC...",
        "tags": ["news", "lore", "high-end"],
        "only_wow": False
    }
}
KEYWORDS = ["WoW", "The War Within", "Classic"]
CUTOFF_DATE = "2023-01-01T00:00:00Z"
LOG_CONFIG_PATH = "logging_config.json"
```