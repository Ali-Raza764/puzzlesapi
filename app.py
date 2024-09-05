from flask import Flask, jsonify, request, render_template
import pandas as pd
import requests
import zstandard as zstd
from io import BytesIO
from flask_caching import Cache

app = Flask(__name__)

# Configure caching
cache = Cache(app, config={'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 300})

# Function to download and load the CSV from a URL
def download_and_load_csv(url):
    try:
        # Step 1: Download the .zst file
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        
        # Step 2: Decompress the .zst file
        dctx = zstd.ZstdDecompressor()
        decompressed_data = dctx.decompress(response.content)
        
        # Step 3: Load the decompressed CSV into a DataFrame
        df = pd.read_csv(BytesIO(decompressed_data))
        df.set_index('PuzzleId', inplace=True)  # Set PuzzleId as the index
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

# URL of the puzzles CSV file
puzzles_url = 'https://database.lichess.org/lichess_db_puzzle.csv.zst'

# Load the CSV into a DataFrame
df = download_and_load_csv(puzzles_url)

@app.route('/puzzles', methods=['GET'])
@cache.cached(query_string=True)
def get_puzzles():
    try:
        # Ensure DataFrame is loaded
        if df is None:
            return jsonify({"error": "Failed to load puzzles data."}), 500

        # Get pagination parameters from query string
        start = request.args.get('start', default=0, type=int)
        limit = request.args.get('limit', default=10, type=int)

        # Get filter parameters from query string
        min_rating = request.args.get('min_rating', default=0, type=int)
        max_rating = request.args.get('max_rating', default=3000, type=int)
        themes = request.args.get('themes', default=None, type=str)

        # Filter the DataFrame based on rating
        filtered_df = df[(df['Rating'] >= min_rating) & (df['Rating'] <= max_rating)]

        # Further filter the DataFrame based on themes if provided
        if themes:
            theme_list = themes.split(',')
            filtered_df = filtered_df[filtered_df['Themes'].apply(lambda x: any(theme in x for theme in theme_list))]

        # Apply pagination
        paginated_df = filtered_df.iloc[start:start + limit]

        # Select specific columns
        puzzles = paginated_df.reset_index()[[
            'PuzzleId', 'FEN', 'Moves', 'Rating', 'RatingDeviation', 'Popularity',
            'NbPlays', 'Themes', 'GameUrl', 'OpeningTags'
        ]].to_dict(orient='records')

        return jsonify(puzzles)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/puzzle', methods=['GET'])
@cache.cached(query_string=True)
def get_puzzle():
    try:
        puzzle_id = request.args.get('puzzle_id', default='', type=str)

        # Ensure DataFrame is loaded
        if df is None:
            return jsonify({"error": "Failed to load puzzles data."}), 500

        # Find the puzzle by ID
        if puzzle_id not in df.index:
            return jsonify({"error": "Puzzle not found"}), 404

        puzzle = df.loc[puzzle_id][[
            'PuzzleId', 'FEN', 'Moves', 'Rating', 'RatingDeviation', 'Popularity',
            'NbPlays', 'Themes', 'GameUrl', 'OpeningTags'
        ]].to_dict()

        return jsonify(puzzle)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/')
def documentation():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
