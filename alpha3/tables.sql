CREATE TABLE IF NOT EXISTS league (
	    league_id SERIAL,
        league VARCHAR(3),
        season VARCHAR(4),
        league_name VARCHAR(50),
        players_location VARCHAR(100),
        match_location VARCHAR(100),
        odds_location VARCHAR(100),
        UNIQUE (league, season),
        PRIMARY KEY (league_id)
);

CREATE TABLE IF NOT EXISTS club (
		club_id SERIAL,
		league_id INTEGER,
		club_name VARCHAR(50),
		PRIMARY KEY (club_id),
		FOREIGN KEY (league_id) REFERENCES league(league_id)
);

CREATE TABLE IF NOT EXISTS player (
		player_id SERIAL,
		name VARCHAR(50),
		club_id INTEGER REFERENCES club(club_id),
		overall_rating INTEGER,
        potential_rating INTEGER,
		position VARCHAR(3),
        age INTEGER,
        value REAL,
        country VARCHAR(50),
        total_rating INTEGER,
		PRIMARY KEY (player_id)
);



CREATE TABLE IF NOT EXISTS match (
		match_id SERIAL,
		home_id INTEGER REFERENCES club(club_id),
		away_id INTEGER REFERENCES club(club_id),
		game_date DATE,
		h1_player_id INTEGER REFERENCES player(player_id),
		h2_player_id INTEGER REFERENCES player(player_id),
		h3_player_id INTEGER REFERENCES player(player_id),
		h4_player_id INTEGER REFERENCES player(player_id),
		h5_player_id INTEGER REFERENCES player(player_id),
		h6_player_id INTEGER REFERENCES player(player_id),
		h7_player_id INTEGER REFERENCES player(player_id),
		h8_player_id INTEGER REFERENCES player(player_id),
		h9_player_id INTEGER REFERENCES player(player_id),
		h10_player_id INTEGER REFERENCES player(player_id),
		h11_player_id INTEGER REFERENCES player(player_id),
		a1_player_id INTEGER REFERENCES player(player_id),
		a2_player_id INTEGER REFERENCES player(player_id),
		a3_player_id INTEGER REFERENCES player(player_id),
		a4_player_id INTEGER REFERENCES player(player_id),
		a5_player_id INTEGER REFERENCES player(player_id),
		a6_player_id INTEGER REFERENCES player(player_id),
		a7_player_id INTEGER REFERENCES player(player_id),
		a8_player_id INTEGER REFERENCES player(player_id),
		a9_player_id INTEGER REFERENCES player(player_id),
		a10_player_id INTEGER REFERENCES player(player_id),
		a11_player_id INTEGER REFERENCES player(player_id),
		home_goals INTEGER,
        away_goals INTEGER,
        home_max REAL,
        draw_max REAL,
        away_max REAL,
        broker_home_max VARCHAR(30),
        broker_draw_max VARCHAR(30),
        broker_away_max VARCHAR(30),
        market_home_max REAL,
        market_draw_max REAL,
        market_away_max REAL,
        max_over_2_5 REAL,
        max_under_2_5 REAL,
		PRIMARY KEY (match_id)
);

CREATE TABLE IF NOT EXISTS club_match (
		club_id INTEGER REFERENCES club(club_id),
		match_id INTEGER REFERENCES match(match_id),
		PRIMARY KEY (club_id, match_id)
);

CREATE TABLE IF NOT EXISTS player_match (
		player_id INTEGER REFERENCES player(player_id),
		match_id INTEGER REFERENCES match(match_id),
		PRIMARY KEY (player_id, match_id)
);