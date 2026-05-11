from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

teams = {
    "RCB": {
        "players":  ["Virat", "Faf", "Rajat", "Glenn", "Dinesh"],
        "style":    "aggressive",   # affects event weights
    },
    "MI": {
        "players":  ["Rohit", "Ishan", "Suryakumar", "Hardik", "Pollard"],
        "style":    "aggressive",
    },
    "CSK": {
        "players":  ["Ruturaj", "Conway", "Shivam", "Dhoni", "Jadeja"],
        "style":    "balanced",
    },
    "GT": {
        "players":  ["Gill", "Wriddhiman", "David", "Rashid", "Mohit"],
        "style":    "defensive",
    },
    "KKR": {
        "players":  ["Venkatesh", "Angkrish", "Rinku", "Sunil", "Varun"],
        "style":    "balanced",
    },
    "DC": {
        "players":  ["Warner", "Jake", "Axar", "Kuldeep", "Anrich"],
        "style":    "defensive",
    },
}

matches = [
    {"match_id": "IPL_2026_M01", "team_a": "RCB", "team_b": "MI"},
    {"match_id": "IPL_2026_M02", "team_a": "CSK", "team_b": "GT"},
    {"match_id": "IPL_2026_M03", "team_a": "KKR", "team_b": "DC"},
]

STYLE_WEIGHTS = {
    "aggressive": [8,  30, 15, 25, 15, 7],
    "balanced":   [12, 35, 15, 20, 10, 8],
    "defensive":  [18, 40, 12, 15,  7, 8],
}

EVENT_TYPES = ["dot", "single", "double", "four", "six", "wicket"]


def pick_event(style: str):
    """Return a random event biased by the team's batting style."""
    return random.choices(EVENT_TYPES, weights=STYLE_WEIGHTS[style])[0]


def build_payload(match: dict, batting_team: str, player: str, event: str) -> dict:
    """Translate a raw event into a match_stats-compatible row."""
    runs = fours = sixes = wickets = 0
    balls = 1                           # every legal delivery counts as 1 ball

    if event == "dot":
        runs = 0
    elif event == "single":
        runs = random.choice([1, 1, 1, 2, 3])   # weighted towards 1
    elif event == "double":
        runs = 2
    elif event == "four":
        runs, fours = 4, 1
    elif event == "six":
        runs, sixes = 6, 1
    elif event == "wicket":
        runs, wickets = 0, 1
        balls = 1

    return {
        "match_id":   match["match_id"],
        "player":     player,
        "team":       batting_team,
        "runs":       runs,
        "balls":      balls,
        "wickets":    wickets,
        "fours":      fours,
        "sixes":      sixes,
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

class MatchState:
    def __init__(self, match: dict):
        self.match    = match
        self.team_a   = match["team_a"]
        self.team_b   = match["team_b"]

        # batting starts with team_a
        self.batting_team = self.team_a
        self.bowling_team = self.team_b

        
        self.orders = {
            t: list(teams[t]["players"]) for t in [self.team_a, self.team_b]
        }
        self.striker_idx     = {t: 0 for t in [self.team_a, self.team_b]}
        self.non_striker_idx = {t: 1 for t in [self.team_a, self.team_b]}
        self.wickets_fallen  = {t: 0 for t in [self.team_a, self.team_b]}

        self.ball_in_over = 0   # 0-5
        self.over         = 0   # 0-19

    @property
    def current_team_players(self):
        return teams[self.batting_team]["players"]

    @property
    def current_style(self):
        return teams[self.batting_team]["style"]

    def striker(self):
        t  = self.batting_team
        idx = self.striker_idx[t] % len(self.orders[t])
        return self.orders[t][idx]

    def non_striker(self):
        t  = self.batting_team
        idx = self.non_striker_idx[t] % len(self.orders[t])
        return self.orders[t][idx]

    def advance(self, event: str, runs: int):
        """Update batting state after each delivery."""
        t = self.batting_team

        if event == "wicket":
            self.wickets_fallen[t] += 1
            next_batter = (self.wickets_fallen[t] + 1) % len(self.orders[t])
            self.striker_idx[t] = next_batter
            # all 10 wickets → team innings over (simple guard)
            if self.wickets_fallen[t] >= len(self.orders[t]) - 1:
                self._swap_innings()
                return

        # rotate strike on odd runs
        if runs % 2 == 1:
            self.striker_idx[t], self.non_striker_idx[t] = \
                self.non_striker_idx[t], self.striker_idx[t]

        self.ball_in_over += 1
        if self.ball_in_over == 6:
            self.ball_in_over = 0
            self.over        += 1
            
            self.striker_idx[t], self.non_striker_idx[t] = \
                self.non_striker_idx[t], self.striker_idx[t]
            
            if self.over >= 20:
                self._swap_innings()

    def _swap_innings(self):
        """Switch batting/bowling teams."""
        self.batting_team, self.bowling_team = self.bowling_team, self.batting_team
        self.ball_in_over = 0
        self.over         = 0



match_states = [MatchState(m) for m in matches]

print("=" * 55)
print("  IPL 2026 Real-Time Data Stream  |  3 Live Matches")
print("=" * 55)
for m in matches:
    print(f"  {m['match_id']}:  {m['team_a']}  vs  {m['team_b']}")
print("=" * 55)


event_counter = 0

while True:
    # pick a random match to emit an event for (simulates parallel live data)
    state = random.choice(match_states)

    event   = pick_event(state.current_style)
    player  = state.striker()

    payload = build_payload(state.match, state.batting_team, player, event)
    state.advance(event, payload["runs"])

    event_counter += 1
    emoji = {"dot": "·", "single": "→", "double": "⇒",
             "four": "4️⃣ ", "six": "6️⃣ ", "wicket": "❌"}
    print(
        f"[{event_counter:>5}]  {state.match['match_id']}  |  "
        f"{player:<14} ({payload['team']:<3})  "
        f"{emoji.get(event, event):<4}  {payload['runs']} runs"
    )

    producer.send("cricket", value=payload)
    producer.flush()

    # emit ~1 event every 2-4 seconds → lively dashboard with realistic pace
    time.sleep(random.uniform(2.0, 4.0))
