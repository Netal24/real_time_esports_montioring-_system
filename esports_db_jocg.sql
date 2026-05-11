DROP TABLE IF EXISTS match_stats;

CREATE TABLE match_stats (
    id SERIAL PRIMARY KEY,
    match_id VARCHAR(20) NOT NULL, 
    player VARCHAR(50) NOT NULL,
    team VARCHAR(50) NOT NULL,
    runs INT DEFAULT 0,            
    balls INT DEFAULT 1,           
    wickets INT DEFAULT 0,        
    fours INT DEFAULT 0,
    sixes INT DEFAULT 0,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);

select * from match_stats;	


