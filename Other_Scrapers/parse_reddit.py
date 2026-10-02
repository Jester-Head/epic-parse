import gzip
import json
import os

def parse_reddit_gz_files(directory, callback):
    """
    Parse all .gz files in the given directory, calling `callback` for each JSON object.
    :param directory: Path to directory containing .gz files
    :param callback: Function to process each JSON object
    """
    for filename in os.listdir(directory):
        if filename.endswith('.gz'):
            filepath = os.path.join(directory, filename)
            with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        callback(data)
                    except json.JSONDecodeError:
                        continue

# Example usage:
if __name__ == '__main__':
    def print_title(obj):
        if 'title' in obj:
            print(obj['title'])

    parse_reddit_gz_files(r"C:\Users\sarah\Downloads\reddit\subreddits24", print_title)