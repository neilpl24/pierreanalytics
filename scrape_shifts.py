import requests
import pandas as pd
import numpy as np
from datetime import datetime
import numbers
import time
import gc

gc.collect()
event_list = []
home_goals = 0
away_goals = 0

csv_shifts = pd.read_csv("missing_shift_data_2024.csv")


def compare_dates(date1, date2):
    format = "%Y-%m-%d"
    datetime1 = datetime.strptime(date1, format)
    datetime2 = datetime.strptime(date2, format)

    if datetime1 <= datetime2:
        return True
    elif datetime1 > datetime2:
        return False


def get_player_name(player_id, roster):
    """Gets player name from player id"""
    if player_id is None:
        return pd.NA
    player = list(
        filter(
            lambda p: p["playerId"] == player_id,
            roster,
        )
    )
    if len(player) == 0:
        return pd.NA
    return f'{player[0]["firstName"]["default"]} {player[0]["lastName"]["default"]}'


def event_scraper(season):
    """Returns a season full of event data."""
    global home_goals, away_goals
    schedule = requests.get(
        f"https://api-web.nhle.com/v1/schedule/{season}-09-01"
    ).json()
    endDate = f"{season+1}-09-01"
    while "nextStartDate" in schedule.keys():
        nextStartDate = schedule["nextStartDate"]
        schedule = requests.get(
            f"https://api-web.nhle.com/v1/schedule/{nextStartDate}"
        ).json()
        if not compare_dates(nextStartDate, endDate):
            break
        days = schedule["gameWeek"]
        for day in days:
            print(day["date"])
            gameIds = get_game_ids(day)
            for gameId in gameIds:
                game_data = get_game_data(gameId, season)
                event_list.extend(game_data)
                away_goals = 0
                home_goals = 0

    df = pd.DataFrame(event_list)
    df.fillna("NA", inplace=True)
    df = df[
        [
            "season",
            "game_date",
            "game_id",
            "arena",
            "home_team_id",
            "away_team_id",
            "home_team_name",
            "away_team_name",
            "team_id",
            "home_goals",
            "away_goals",
            "game_type",
            "event_id",
            "goal_highlight_id",
            "period",
            "period_type",
            "start_time",
            "event_type",
            "event_player_1",
            "event_player_1_name",
            "event_player_2",
            "event_player_2_name",
            "event_player_3",
            "zone",
            "x",
            "y",
            "shot_type",
            "goalie_id",
            "goalie_name",
            "shift_start",
            "team_skaters",
            "opposing_skaters",
            "strength",
            "skater_state",
            "duration",
        ]
    ]
    df.to_csv(f"./season_data/{season}{season+1}.csv", index=False)


def get_game_ids(date):
    """Grabs game IDs given a date"""
    games = date["games"]
    gameIDs = list(map(lambda game: game["id"], games))
    # No preseason or all star games
    gameIDs = list(
        filter(
            lambda gamePk: str(gamePk)[4:6] == "02" or str(gamePk)[4:6] == "03",
            gameIDs,
        )
    )
    return gameIDs


def skater_to_team_map(pbp):
    team_map = {}
    for player in pbp["rosterSpots"]:
        team_map[player["playerId"]] = player["teamId"]
    return team_map


def skater_to_position_map(pbp):
    roster_map = {}
    for player in pbp["rosterSpots"]:
        roster_map[player["playerId"]] = player["positionCode"]
    return roster_map


def apply_game_data(game_data, event, season):
    """Fills in remaining columns for an event"""
    event["game_id"] = game_data["id"]
    event["game_date"] = game_data["gameDate"]
    event["arena"] = game_data["venue"]["default"]
    event["home_team_id"] = game_data["homeTeam"]["id"]
    event["home_team_name"] = (
        game_data["homeTeam"]["placeName"]["default"]
        + " "
        + game_data["homeTeam"]["commonName"]["default"]
    )
    event["away_team_name"] = (
        game_data["awayTeam"]["placeName"]["default"]
        + " "
        + game_data["awayTeam"]["commonName"]["default"]
    )
    event["away_team_id"] = game_data["awayTeam"]["id"]
    event["game_type"] = (
        "Regular Season" if str(game_data["id"])[4:6] == "02" else "Playoffs"
    )
    event["season"] = f"{season}{season+1}"

    return event


def get_skaters_for_event(period_time, shifts, period, team_id, team_map, roster_map):
    """Gets the skaters on the ice for a change or event"""
    team_arr = set()
    opposing_arr = set()

    for shift in shifts:
        shift_start = shift["start_time"]
        shift_end = shift["end_time"]
        if (
            shift["period"] == period
            and shift_start < period_time
            and shift_end >= period_time
        ):
            if (
                team_map.get(shift["playerId"]) == None
                or roster_map[shift["playerId"]] == "G"
            ):
                continue
            player_team = team_map[shift["playerId"]]
            if player_team == team_id:
                team_arr.add(shift["playerId"])
            else:
                opposing_arr.add(shift["playerId"])

    return {
        "team_skaters": list(team_arr),
        "opposing_skaters": list(opposing_arr),
    }


def get_skater_state(team_arr, opposing_arr):
    """Returns the strength of an event"""
    team_skaters = len(team_arr)
    opposing_skaters = len(opposing_arr)
    return f"{team_skaters}v{opposing_skaters}"


def get_strength_state(team_arr, opposing_arr):
    """Returns the strength of an event"""
    team_skaters = len(team_arr)
    opposing_skaters = len(opposing_arr)
    if team_skaters == opposing_skaters:
        return "EV"
    elif team_skaters > opposing_skaters:
        return "Powerplay"
    else:
        return "Shorthanded"


def get_game_data(game_id, season):
    """Grabs shifts and PBP data for a given game"""
    # I hate doing this but fuck it
    while True:
        try:
            shifts_data = requests.get(
                f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}"
            ).json()
            pbp = requests.get(
                f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
            ).json()
            break
        except (
            requests.exceptions.RequestException,
            ConnectionResetError,
        ) as err:
            time.sleep(10)
            print("Taking a break...")

    events = pbp["plays"]
    shifts = shifts_data["data"]
    team_map = get_team_ids(pbp)
    player_team_map = skater_to_team_map(pbp)
    roster_team_map = skater_to_position_map(pbp)
    if len(shifts) == 0:
        shifts = use_back_up_shifts(game_id, team_map)
    # Transform shift data
    for i, shift in enumerate(shifts):
        shift["shift_start"] = get_shift_state(shift, events)
        shifts[i] = transform_shift_times(shift)

    # Order by time and period
    for i, event in enumerate(events):
        events[i] = transform_pbp(event, pbp)
    shifts_and_events = sorted(
        shifts + events, key=lambda x: (x["period"], x["start_time"])
    )

    for occurence in shifts_and_events:
        period_time = occurence["start_time"]
        period = occurence["period"]
        team_id = occurence["team_id"]
        if team_id == pbp["homeTeam"]["id"]:
            occurence["team"] = (
                pbp["homeTeam"]["placeName"]["default"]
                + " "
                + pbp["homeTeam"]["commonName"]["default"]
            )
        else:
            occurence["team"] = (
                pbp["awayTeam"]["placeName"]["default"]
                + " "
                + pbp["awayTeam"]["commonName"]["default"]
            )

        occurence = apply_game_data(pbp, occurence, season)

        state = get_skaters_for_event(
            period_time, shifts, period, team_id, player_team_map, roster_team_map
        )
        occurence["team_skaters"] = state["team_skaters"]
        occurence["opposing_skaters"] = state["opposing_skaters"]
        occurence["skater_state"] = get_skater_state(
            state["team_skaters"], state["opposing_skaters"]
        )
        occurence["strength"] = get_strength_state(
            state["team_skaters"], state["opposing_skaters"]
        )

    return shifts_and_events


def transform_pbp(event, game):
    """Allows us to order events and shifts"""
    global home_goals, away_goals
    event["period"] = event["periodDescriptor"]["number"]
    event["period_type"] = event["periodDescriptor"]["periodType"]
    event["event_id"] = event["eventId"]
    event["start_time"] = convert_time_to_seconds(event["timeInPeriod"])
    event["event_type"] = event["typeDescKey"]
    details = event.get("details", {})
    event["goal_highlight_id"] = details.get("highlightClip", pd.NA)
    event["event_player_1"] = next(
        (
            details.get(key)
            for key in [
                "hittingPlayerId",
                "scoringPlayerId",
                "shootingPlayerId",
                "winningPlayerId",
                "playerId",
                "committedByPlayerId",
            ]
            if details.get(key) is not None
        ),
        None,
    )
    event["event_player_1_name"] = get_player_name(
        event["event_player_1"], game["rosterSpots"]
    )
    event["event_player_2"] = next(
        (
            details.get(key)
            for key in [
                "hitteePlayerId",
                "blockingPlayerId",
                "assist1PlayerId",
                "losingPlayerId",
                "drawnByPlayerId",
            ]
            if details.get(key) is not None
        ),
        None,
    )
    event["event_player_2_name"] = get_player_name(
        event["event_player_2"], game["rosterSpots"]
    )
    event["event_player_3"] = next(
        (
            details.get(key)
            for key in [
                "assist2PlayerId",
            ]
            if details.get(key) is not None
        ),
        None,
    )
    event["zone"] = details.get("zoneCode")
    event["x"] = details.get("xCoord")
    event["y"] = details.get("yCoord")
    event["shot_type"] = details.get("shotType")
    event["goalie_id"] = details.get("goalieInNetId")
    event["goalie_name"] = get_player_name(event["goalie_id"], game["rosterSpots"])
    event["team_id"] = details.get("eventOwnerTeamId")

    if event["typeDescKey"] == "goal":
        if event["team_id"] == game["homeTeam"]["id"]:
            home_goals += 1
        else:
            away_goals += 1
    event["away_goals"] = away_goals
    event["home_goals"] = home_goals

    keys_to_remove = [
        "eventId",
        "periodDescriptor",
        "timeInPeriod",
        "zoneCode",
        "timeRemaining",
        "typeCode",
        "typeDescKey",
        "sortOrder",
        "details",
        "shootingPlayerId",
    ]

    event = {k: v for k, v in event.items() if k not in keys_to_remove}

    return event


def get_team_ids(game):
    """Gets a map of team ID -> team name"""
    team_map = {}
    away_team_id = game["awayTeam"]["id"]
    home_team_id = game["homeTeam"]["id"]
    away_team_name = (
        game["awayTeam"]["placeName"]["default"]
        + " "
        + game["awayTeam"]["commonName"]["default"]
    )
    home_team_name = (
        game["homeTeam"]["placeName"]["default"]
        + " "
        + game["homeTeam"]["commonName"]["default"]
    )

    team_map["away"] = away_team_id
    team_map["home"] = home_team_id

    return team_map


def get_rosters(game):
    """Gets a map of player ID -> team and name"""
    roster_spots = game["rosterSpots"]
    roster_map = {}
    for player in roster_spots:
        roster_map[player["playerId"]] = {"team_id": player["teamId"]}
    return roster_map


def get_shift_state(shift, events):
    period = shift["period"]
    team_id = shift["teamId"]
    start_time = shift["startTime"]

    events = list(
        filter(
            lambda event: event["timeInPeriod"] == start_time
            and event["periodDescriptor"]["number"] == period
            and event["typeDescKey"] == "faceoff",
            events,
        )
    )
    if len(events) == 0:
        return "F"
    faceoff = events[0]
    zone = faceoff["details"]["zoneCode"]
    event_team = faceoff["details"]["eventOwnerTeamId"]
    if zone == "O":
        return "O" if event_team == team_id else "D"
    elif zone == "D":
        return "D" if event_team == team_id else "O"
    else:
        return "N"


def transform_shift_times(shift):
    """Transforms all of the shift time fields from mm:ss to seconds"""
    shift["duration"] = convert_time_to_seconds(shift["duration"])
    shift["start_time"] = convert_time_to_seconds(shift["startTime"])
    shift["end_time"] = convert_time_to_seconds(shift["endTime"])
    shift["team_id"] = shift["teamId"]
    shift["event_type"] = "line_change"

    # Remove unnecessary keys
    removed_keys = [
        "id",
        "detailCode",
        "eventDescription",
        "eventDetails",
        "firstName",
        "gameId",
        "hexValue",
        "lastName",
        "shiftNumber",
        "startTime",
        "teamAbbrev",
        "endTime",
        "eventDescription",
        "typeCode",
        "teamId",
        "teamName",
    ]
    shift = {k: v for k, v in shift.items() if k not in removed_keys}
    return shift


def use_back_up_shifts(game_id, team_map):
    """Unfortunately, some games in the NHL API from the 24-25 season are missing shifts.
    Was lucky to obtain a CSV for missing shifts from @yimmymcbill on Twitter who connected this API to the HTML shifts
    """
    shifts = csv_shifts[csv_shifts["gameId"] == game_id].copy()
    shifts["duration"] = shifts["endTime"] - shifts["startTime"]
    shifts["teamId"] = shifts["team"].map(team_map)
    return shifts.to_dict(orient="records")


def convert_time_to_seconds(str):
    if str is None or str == "":
        return 0
    if isinstance(str, numbers.Number):
        return str
    minutes, seconds = map(int, str.split(":"))
    total_seconds = (minutes * 60) + seconds
    return total_seconds


for year in range(2010, 2020):
    event_list = []
    event_scraper(year)
# event_scraper(2020)
for year in range(2024, 2025):
    event_list = []
    event_scraper(year)
