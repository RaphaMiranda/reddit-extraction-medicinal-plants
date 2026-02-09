import duckdb
import os
import sys
import time
import json
from datetime import datetime

# --- Import Setup ---
# Assumes 'yars' package is in the same directory or PYTHONPATH
try:
    from yars.src.yars import YARS
except ImportError:
    print("Error: Could not import YARS. Make sure 'yars' package is available.")
    sys.exit(1)

# --- Configuration ---
# Ensure data directory exists
if not os.path.exists("data"):
    os.makedirs("data")

OUTPUT_DB = os.path.join("data", "medicinal_data.duckdb")
RAW_RESULTS_FILE = os.path.join("data", "raw_search_results.json")
TABLE_NAME = "medicinal_mentions"
SEARCH_LIMIT_PER_PLANT = 500 # Number of posts to fetch per plant keyword

# Keyword Groups (Case-insensitive)
# English Group Words

GROUP_A_PLANTS = {
    "Emilia sonchifolia", "Lilac Tasselflower", "Cupid's Shaving Brush", "Flora's Paintbrush", 
    "Purple Sow Thistle", "Red Tasselflower", "Red Groundsel", "Emilia fosbergii", "Florida Tasselflower",
    "Tasselflower", "Flora's Paintbrush", "Purple Emilia", "Red Sow Thistle", "Fosberg's Emilia"
}

"""
GROUP_B_MEDICINAL = {
    "medicinal", "medicine", "medication", "remedy", "remedies", "natural remedy", "herbal remedy", 
    "cure", "cures", "curative", "treatment", "treatments", "treat", "treating", "therapy", 
    "therapeutic", "healing", "heals", "heal", "health benefit", "health benefits", "used for", 
    "helps with", "helps", "relieves", "relief", "prevent", "prevention", "manage symptoms", 
    "traditional medicine", "folk medicine", "home remedy", "alternative medicine", 
    "complementary medicine", "plant medicine", "herbal medicine", "natural medicine", 
    "ethnomedicine", "botanical medicine", "supplement", "supplements", "dietary supplement", 
    "nutritional supplement", "food as medicine", "functional food", "edible", "eaten", "consume", 
    "consumed", "consumption", "ingest", "ingestion", "brew", "brewed", "tea", "herbal tea", 
    "infusion", "decoction", "extract", "tincture", "powder", "capsule", "dose", "dosage", 
    "taken for", "recommended for", "recommend", "recommended", "recommendation", "prescribed", 
    "self-medication", "traditional use", "used traditionally", "ancestral knowledge", 
    "popular knowledge", "folk knowledge", "natural treatment", "plant-based treatment", 
    "healing plant", "medicinal plant", "toxic", "toxicity", "poisonous", "plant toxin", 
    "toxic compound", "side effects", "adverse effects", "harmful", "unsafe", "risk", "warning", 
    "hepatotoxic", "liver toxicity", "liver damage", "liver injury", "alkaloid", 
    "pyrrolizidine alkaloid", "toxic alkaloid"
}
"""

# Portuguese Group Words
"""
GROUP_A_PLANTS = {
    "Serralha-mirim", "Serralha mirim", "Bela-emília", "Bela emília", "Erva-do-fígado", "Erva do fígado",
    "Serralhinha", "Falsa-serralha", "Falsa serralha", "Flor Pincel", "Flor-pincel", 
    "pincel-de-estudante", "pincel de estudante", "algodão de preá"
}
"""


def contains_keywords(text):
    """
    Checks if text contains at least one keyword from Group A AND at least one from Group B.
    Returns: (bool, matched_A, matched_B)
    """
    if not text:
        return False, [], []
    
    text_lower = text.lower()
    
    matched_a = [k for k in GROUP_A_PLANTS if k.lower() in text_lower]

    # If Group B is empty or None, we only care about Group A matches
    group_b = globals().get("GROUP_B_MEDICINAL")
    if not group_b:
        return len(matched_a) > 0, matched_a, []

    matched_b = [k for k in group_b if k in text_lower]
    
    # Logic: Must contain at least one from A AND at least one from B
    return (len(matched_a) > 0 and len(matched_b) > 0), matched_a, matched_b

def init_db(db_path):
    """Initialize DuckDB table if it doesn't exist."""
    conn = duckdb.connect(db_path)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (id VARCHAR, type VARCHAR, author VARCHAR, created_utc TIMESTAMP, content VARCHAR, matched_plants VARCHAR, matched_medicinal VARCHAR, parent_id VARCHAR, permalink VARCHAR, query VARCHAR)")
    conn.close()

def get_existing_ids(db_path):
    """Returns a set of IDs already in the database."""
    if not os.path.exists(db_path):
        return set()
    
    conn = duckdb.connect(db_path)
    # Check if table exists
    try:
        existing = conn.execute(f"SELECT id FROM {TABLE_NAME}").fetchall()
        conn.close()
        return set(row[0] for row in existing)
    except duckdb.CatalogException:
        conn.close()
        return set()

def save_items_to_db(items, db_path):
    """Appends items to DuckDB."""
    if not items:
        return

    conn = duckdb.connect(db_path)
    
    data_tuples = []
    for item in items:
        data_tuples.append((
            item['id'], item['type'], item['author'], item['created_utc'], item['content'], 
            item['matched_plants'], item['matched_medicinal'], item['parent_id'], item['permalink'], item['query']
        ))
        
    conn.executemany(f"INSERT INTO {TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data_tuples)
    print(f"Saved {len(items)} items to DB.")
    conn.close()

def save_raw_results(results, plant_name, file_path):
    """Appends raw search results to a JSON file."""
    if not results:
        return

    # Prepare data structure
    entry = {
        "timestamp": datetime.now().isoformat(),
        "plant_query": plant_name,
        "result_count": len(results),
        "results": results
    }

    # Lock file or just append safely? For simplicity, we will read, append, write.
    # Warning: Not thread-safe, but fine for single process.
    data = []
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, list):
                    data = []
        except (json.JSONDecodeError, ValueError):
            data = []
    
    data.append(entry)
    
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    print(f"  Saved raw results to {file_path}")

def extract_timestamp(utc_val):
    if utc_val:
        try:
            return datetime.fromtimestamp(utc_val)
        except:
            return None
    return None

def flatten_comments(comments, parent_id, post_permalink, query_plant):
    flat_list = []
    for comment in comments:
        # Check keyword matches in comment body
        body = comment.get('body', '')
        is_match, match_a, match_b = contains_keywords(body)
        
        created_utc = extract_timestamp(comment.get('created_utc'))
            
        flat_list.append({
            'id': comment.get('id', 'unknown'),
            'type': 'comment',
            'author': comment.get('author', 'unknown'),
            'created_utc': created_utc,
            'content': body,
            'matched_plants': ", ".join(match_a),
            'matched_medicinal': ", ".join(match_b),
            'parent_id': parent_id,
            'permalink': f"https://www.reddit.com{comment.get('permalink')}" if comment.get('permalink') else post_permalink,
            'query': query_plant
        })
        
        if comment.get('replies'):
            flat_list.extend(flatten_comments(comment['replies'], comment.get('id'), post_permalink, query_plant))
            
    return flat_list

def process_post(post, miner, query_plant):
    """
    Process a single post:
    1. Fetch full details including comments.
    2. Check post content for keywords.
    3. Check comments for keywords.
    4. If ANY match found (Post or Comment), save the Post and the matching Comments.
    """
    extracted_items = []
    
    # Basic info from search result
    post_id = post.get('id', post.get('permalink', 'unknown'))
    permalink = post.get('permalink', '')
    if not permalink.startswith('http'):
        permalink = f"https://www.reddit.com{permalink}"
    
    title = post.get('title', '')
    search_body = post.get('selftext', post.get('body', '')) 
    
    print(f"  Checking post: {title[:50]}...")
    
    # Fetch full details (includes full body and comments)
    details = None
    retries = 3
    while retries > 0:
        try:
           details = miner.scrape_post_details(post.get('permalink', ''))
           break
        except Exception as e:
           if "429" in str(e):
               print("    [429] Too Many Requests. Sleeping for 20s...")
               time.sleep(20)
               retries -= 1
           else:
               print(f"    Error scraping details: {e}")
               break
    
    full_body = search_body
    comments = []
    
    if details:
        full_body = details.get('body', search_body)
        comments = details.get('comments', [])
    else:
        print("    Failed to fetch details. Using search result summary.")

    full_text = f"{title} {full_body}"
    
    # 1. Check Post
    post_is_match, post_match_a, post_match_b = contains_keywords(full_text)
    
    # 2. Check Comments
    matched_comments = []
    if comments:
        # We use a helper that processes the list and returns ONLY matching comments flattened
        # But we need to define the logic here or use flatten_comments but filter it 
        # Actually flatten_comments currently returns ALL? No, let's look at flatten_comments.
        # Wait, flatten_comments in previous code did "is_match... if comment...".
        # It didn't filter? Let's check previous implementation.
        # Previous flatten_comments checks keywords and appends. It seems it doesn't filter out non-matches?
        # Re-reading flatten_comments: It appends `flat_list.append`. It writes matched_plants. 
        # It does NOT filter based on is_match. It adds everything.
        # We likely only want to save comments that MATCH.
        pass

    # Let's redefine how we collect comments to strictly only keep matches or keep all if we want context.
    # Usually for "extraction" we want matches.
    
    # Helper to process and filter comments
    def get_matching_comments_recursive(comment_list, parent_id):
        matches = []
        for comment in comment_list:
            c_body = comment.get('body', '')
            c_is_match, c_match_a, c_match_b = contains_keywords(c_body)
            
            # Recurse first
            replies_matches = []
            if comment.get('replies'):
                replies_matches = get_matching_comments_recursive(comment['replies'], comment.get('id'))
            
            # If this comment matches OR has matching replies (optional context?), let's stick to strict matching for now.
            # If the user wants "results", they probably want the text containing the keyword.
            
            if c_is_match:
                c_created = extract_timestamp(comment.get('created_utc'))
                matches.append({
                    'id': comment.get('id', 'unknown'),
                    'type': 'comment',
                    'author': comment.get('author', 'unknown'),
                    'created_utc': c_created,
                    'content': c_body,
                    'matched_plants': ", ".join(c_match_a),
                    'matched_medicinal': ", ".join(c_match_b),
                    'parent_id': parent_id,
                    'permalink': f"https://www.reddit.com{comment.get('permalink')}" if comment.get('permalink') else permalink,
                    'query': query_plant
                })
            
            matches.extend(replies_matches)
        return matches

    matched_comment_items = get_matching_comments_recursive(comments, post_id)
    
    # Deciding what to save
    # If Post matches OR we have matching comments
    if post_is_match or matched_comment_items:
        if post_is_match:
            print("    [MATCH] Post matches.")
        if matched_comment_items:
             print(f"    [MATCH] Found {len(matched_comment_items)} matching comments.")

        created_utc = extract_timestamp(post.get('created_utc'))
        
        # We always save the post if there is ANY match related to it, for context/parent reference.
        post_item = {
            'id': post_id,
            'type': 'post',
            'author': post.get('author', 'unknown'),
            'created_utc': created_utc,
            'content': full_text,
            'matched_plants': ", ".join(post_match_a),
            'matched_medicinal': ", ".join(post_match_b),
            'parent_id': None,
            'permalink': permalink,
            'query': query_plant
        }
        extracted_items.append(post_item)
        extracted_items.extend(matched_comment_items)
        
    return extracted_items

def run_extraction():
    print("Initializing YARS miner...")
    miner = YARS()
    
    print(f"Initializing DuckDB: {OUTPUT_DB}...")
    init_db(OUTPUT_DB)
    
    # Load existing IDs
    existing_ids = get_existing_ids(OUTPUT_DB)
    print(f"Loaded {len(existing_ids)} existing IDs from DB.")
    
    total_matches = 0
    
    # Iterate through Group A plants
    for plant in GROUP_A_PLANTS:
        print(f"\nSearching for plant: '{plant}'...")
        try:
            # Search Reddit for the plant name
            results = miner.search_reddit(plant, limit=SEARCH_LIMIT_PER_PLANT)
            
            if not results:
                print("  No results found.")
                continue
                
            print(f"  Found {len(results)} raw results. Saving raw data and filtering for matches...")
            
            # Save raw results
            save_raw_results(results, plant, RAW_RESULTS_FILE)
            
            items_to_save = []
            
            for post in results:
                # Optimization: Skip if post ID already exists and we assume we have processed it
                # Note: This means we won't update comments for existing posts. 
                # Given 'append new results' directive, this is appropriate.
                # Use raw ID from search result which matches the ID we store for type='post'
                post_id = post.get('id')
                # Also check permalink based ID if needed (fallback in process_post)
                if not post_id:
                     post_id = post.get('permalink')

                if post_id in existing_ids:
                    # We've seen this post. Skip processing.
                    continue

                items = process_post(post, miner, plant)
                
                # Check each item (post and comments) against existing_ids
                # (Though if we skipped the post above, we won't be here, but good for safety)
                for item in items:
                    if item['id'] not in existing_ids:
                        items_to_save.append(item)
                        existing_ids.add(item['id'])
            
            if items_to_save:
                save_items_to_db(items_to_save, OUTPUT_DB)
                total_matches += len(items_to_save)
            else:
                print("  No new matching results found.")
                
            # Be nice to Reddit API
            time.sleep(2) 
            
        except Exception as e:
            print(f"  Error searching for '{plant}': {e}")
            import traceback
            traceback.print_exc()
            
    print(f"\nExtraction complete. Total new matches saved: {total_matches}")

if __name__ == "__main__":
    run_extraction()
