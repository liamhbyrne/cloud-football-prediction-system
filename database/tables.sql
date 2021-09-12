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
        UNIQUE (league_id, club_name),
        PRIMARY KEY (club_id),
        FOREIGN KEY (league_id) REFERENCES league(league_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS player (
        player_id SERIAL,
        name VARCHAR(50),
        club_id INTEGER REFERENCES club(club_id) ON DELETE CASCADE,
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
        home_id INTEGER REFERENCES club(club_id) ON DELETE CASCADE,
        away_id INTEGER REFERENCES club(club_id) ON DELETE CASCADE,
        game_date DATE,
        status VARCHAR(20),
        link VARCHAR(200),
        h1_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        h2_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        h3_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        h4_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        h5_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        h6_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        h7_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        h8_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        h9_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        h10_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        h11_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        a1_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        a2_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        a3_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        a4_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        a5_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        a6_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        a7_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        a8_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        a9_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        a10_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
        a11_player_id INTEGER REFERENCES player(player_id) ON DELETE CASCADE,
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
        PRIMARY KEY (match_id),
        UNIQUE (home_id, away_id, game_date)
);
