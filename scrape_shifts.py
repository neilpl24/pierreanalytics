import requests
import pandas as pd
import numpy as np
from datetime import datetime
import time

event_list = []
home_goals = 0
away_goals = 0


def compare_dates(date1, date2):
    format = "%Y-%m-%d"
    datetime1 = datetime.strptime(date1, format)
    datetime2 = datetime.strptime(date2, format)

    if datetime1 <= datetime2:
        return True
    elif datetime1 > datetime2:
        return False


def event_scraper(season):
    """Returns a season full of event data."""
    schedule = requests.get(
        f"https://api-web.nhle.com/v1/schedule/{season}-09-01"
    ).json()
    endDate = "2011-07-01"
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
            game_data = get_game_data(gameId)
            event_list.extend(game_data)

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
            "home_goals",
            "away_goals",
            "game_type",
            "period",
            "period_type",
            "start_time",
            "event_type",
            "event_player_1",
            "event_player_2",
            "zone",
            "x",
            "y",
            "shot_type",
            "goalie_id",
            "shift_start",
            "team_skaters",
            "opposing_skaters",
            "strength",
            "duration",
        ]
    ]
    df.to_csv("./season_data/20102011.csv", index=False)


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


def apply_game_data(game_data, event):
    """Fills in remaining columns for an event"""
    event["game_id"] = game_data["id"]
    event["game_date"] = game_data["gameDate"]
    event["arena"] = game_data["venue"]["default"]
    event["home_team_id"] = game_data["homeTeam"]["id"]
    event["away_team_id"] = game_data["awayTeam"]["id"]
    event["game_type"] = (
        "Regular Season" if str(game_data["id"])[4:6] == "02" else "Playoffs"
    )
    event["season"] = "20102011"

    return event


def get_skaters_for_event(period_time, shifts, period, team_id):
    """Gets the skaters on the ice for a change or event"""
    team_arr = []
    opposing_arr = []

    for shift in shifts:
        shift_start = shift["start_time"]
        shift_end = shift["end_time"]

        if (
            shift["period"] == period
            and shift_start < period_time
            and period_time <= shift_end
        ):
            player_team = shift["team_id"]

            if player_team == team_id:
                team_arr.append(shift["playerId"])
            else:
                opposing_arr.append(shift["playerId"])

    return {
        "team_skaters": team_arr,
        "opposing_skaters": opposing_arr,
    }


def get_strength_state(team_arr, opposing_arr):
    """Returns the strength of an event"""
    team_skaters = len(team_arr)
    opposing_skaters = len(opposing_arr)
    return f"{team_skaters}v{opposing_skaters}"


def get_game_data(gameId):
    """Grabs shifts and PBP data for a given game"""
    try:
        shifts_data = requests.get(
            f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gameId}"
        ).json()
        pbp = requests.get(
            f"https://api-web.nhle.com/v1/gamecenter/{gameId}/play-by-play"
        ).json()
    except (
        requests.exceptions.RequestException,
        ConnectionResetError,
    ) as err:
        time.sleep(10)
        print("Taking a break...")

    events = pbp["plays"]
    shifts = shifts_data["data"]
    # Transform shift data
    for i, shift in enumerate(shifts):
        shift["shift_start"] = get_shift_state(shift, events)
        shifts[i] = transform_shift_times(shift)

    # Order by time and period
    for i, event in enumerate(events):
        events[i] = transform_pbp(event, pbp)
    for item in shifts + events:
        if "start_time" not in item:
            print(item)
    shifts_and_events = sorted(
        shifts + events, key=lambda x: (x["period"], -x["start_time"])
    )

    for occurence in shifts_and_events:
        period_time = occurence["start_time"]
        period = occurence["period"]
        team_id = occurence["team_id"]

        occurence = apply_game_data(pbp, occurence)

        state = get_skaters_for_event(period_time, shifts, period, team_id)
        occurence["team_skaters"] = state["team_skaters"]
        occurence["opposing_skaters"] = state["opposing_skaters"]
        occurence["strength"] = get_strength_state(
            state["team_skaters"], state["opposing_skaters"]
        )

    return shifts_and_events


def transform_pbp(event, game):
    """Allows us to order events and shifts"""
    global home_goals, away_goals
    event["period"] = event["periodDescriptor"]["number"]
    event["period_type"] = event["periodDescriptor"]["periodType"]
    event["start_time"] = convert_time_to_seconds(event["timeRemaining"])
    event["event_type"] = event["typeDescKey"]
    details = event.get("details", {})
    event["event_player_1"] = next(
        (
            details.get(key)
            for key in [
                "hittingPlayerId",
                "shootingPlayerId",
                "winningPlayerId",
                "playerId",
                "committedByPlayerId",
            ]
            if details.get(key) is not None
        ),
        None,
    )
    event["event_player_2"] = next(
        (
            details.get(key)
            for key in [
                "hitteePlayerId",
                "blockingPlayerId" "losingPlayerId",
                "drawnByPlayerId",
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


def get_team_name(game):
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

    team_map[away_team_id] = away_team_name
    team_map[home_team_id] = home_team_name

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


def convert_time_to_seconds(str):
    if str is None:
        return 0
    minutes, seconds = map(int, str.split(":"))
    total_seconds = (minutes * 60) + seconds
    return total_seconds


event_scraper("2010")
# event_scraper('20152016')
# event_scraper('20162017')
# event_scraper('20172018')
# event_scraper('20182019')
# event_scraper('20192020')
# event_scraper('20202021')
# event_scraper('20212022')
# event_scraper("20222023")
