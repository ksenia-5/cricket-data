CREATE TABLE IF NOT EXISTS scores (
	ID INTEGER PRIMARY KEY AUTOINCREMENT,
	BatterName VARCHAR(255) NOT NULL,
	IsOutNote VARCHAR(255) NOT NULL,
    IsOut BOOLEAN NOT NULL,
	RunCount integer NOT NULL,
	BallCount integer NOT NULL,
	Fours integer NOT NULL,
	Sixes integer NOT NULL,
	TeamName VARCHAR(255) NOT NULL,
	MatchID INT NOT NULL,
	GameID INT NOT NULL,
	MatchTitle VARCHAR(255) NOT NULL,
	LeagueYear INT NOT NULL
);