CREATE TABLE relationship
(
    trait1_id BIGINT      NOT NULL,
    trait2_id BIGINT      NOT NULL,
    name      VARCHAR(30) NOT NULL,
    CONSTRAINT fk_trait1 FOREIGN KEY (trait1_id) REFERENCES trait (id),
    CONSTRAINT fk_trait2 FOREIGN KEY (trait2_id) REFERENCES trait (id)
);