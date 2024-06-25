CREATE TABLE entity_type
(
    id   SERIAL PRIMARY KEY,
    name VARCHAR(30) NOT NULL,
    data JSONB       NOT NULL
);