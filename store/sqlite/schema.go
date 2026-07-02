package sqlite

const schema = `
  CREATE TABLE IF NOT EXISTS state (
        id   INTEGER PRIMARY KEY CHECK (id = 1),
        data BLOB NOT NULL
  );`
