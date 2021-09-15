from typing import List, Dict


class Player:
    """
    Class (struct) of all information related to a single player
    """
    def __init__(self, player_id, name, club_id, overall_rating, potential_rating,
                 position, age, value, nationality, total_rating):

        self._player_id         : int = player_id
        self._name              : str = name
        self._club_id           : int = club_id
        self._overall_rating    : int = overall_rating
        self._potential_rating  : int = potential_rating
        self._position          : str = position
        self._age               : int = age
        self._value             : int = value
        self._nationality       : str = nationality
        self._total_rating      : int = total_rating

    def getPlayerID(self) -> int:
        return self._player_id

    def getName(self)  -> str:
        return self._name

    def getClubId(self)  -> int:
        return self._club_id

    def getOverallRating(self)  -> int:
        return self._overall_rating

    def getPotentialRating(self)  -> int:
        return self._potential_rating

    def getPosition(self)  -> str:
        return self._position

    def getAge(self) -> int:
        return self._age

    def getValue(self) -> int:
        return self._value

    def getNationality(self) -> str:
        return self._nationality

    def getTotalRating(self) -> int:
        return self._total_rating


class Team:
    """

    """
    def __init__(self, club_id, club_name):
        self._players   : List  = []
        self._club_id   : int   = club_id
        self._club_name : str   = club_name
        POSITIONS       : Dict[str: List[str,]] = {'GOALKEEPER': ['GK'],
                                                  'DEFENCE': ['RB', 'RWB', 'CB', 'LB', 'LWB'],
                                                  'MIDFIELD': ['CDM', 'LM', 'CM', 'RM', 'CAM'],
                                                  'FORWARD': ['LW', 'CF', 'RW', 'ST']}

    def getPlayers(self) -> List:
        return self._players

    def getClubId(self) -> int:
        return self._club_id

    def getClubName(self) -> str:
        return self._club_name

    def addPlayer(self, player : Player):
        self._players.append(player)


class Match:
    """

    """
    def __init__(self, match_id, home_team : Team, away_team : Team, game_date, status, link,
                 home_goals, away_goals, odds_data):
        self._match_id   : int = match_id
        self._home_team  : Team = home_team
        self._away_team  : Team = away_team
        self._game_date  : str = game_date
        self._status     : str = status
        self._link       : str = link
        self._home_goals : int = home_goals
        self._away_goals : int = away_goals
        self._odds_data  : Dict[str:float] = odds_data

    def aggregateFeatures(self):
        pass

    def getMatchId(self) -> int:
        return self._match_id

    def getHomeTeam(self) -> Team:
        return self._home_team

    def getAwayTeam(self) -> Team:
        return self._away_team

    def getGameDate(self) -> str:
        return self._game_date

    def getStatus(self) -> str:
        return self._status

    def getLink(self) -> str:
        return self._link

    def getHomeGoals(self) -> int:
        return self._home_goals

    def getAwayGoals(self) -> int:
        return self._away_goals

    def getOddsData(self) -> int:
        return self._odds_data
