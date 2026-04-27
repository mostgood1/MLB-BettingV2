import requests
import json

try:
    url = "http://127.0.0.1:5051/api/live_player_lens?date=2026-04-25"
    resp = requests.get(url).json()
    if not resp:
        print("No data returned")
        exit()
    
    first_game = resp[0]
    rows = first_game.get('rows', [])
    
    top_5 = []
    for r in rows[:5]:
        top_5.append({
            "player": r.get("player"),
            "stat": r.get("stat"),
            "recommendation_priority_score": r.get("recommendation_priority_score"),
            "live_rank_probability": r.get("live_rank_probability"),
            "win_prob": r.get("win_prob"),
            "bettable_score": r.get("bettable_score"),
            "first_seen_at": r.get("first_seen_at"),
            "last_seen_at": r.get("last_seen_at"),
            "seen_observations": r.get("seen_observations")
        })
    
    print(json.dumps(top_5, indent=2))
    
    ordered = True
    for i in range(len(rows) - 1):
        if float(rows[i].get('recommendation_priority_score', 0)) < float(rows[i+1].get('recommendation_priority_score', 0)):
            ordered = False
            break
    print(f"Ordered: {ordered}")
except Exception as e:
    print(f"Error: {e}")
