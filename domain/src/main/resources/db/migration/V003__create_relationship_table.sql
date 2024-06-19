CREATE TABLE relationship
(
    subject_id BIGINT      NOT NULL,
    object_id BIGINT      NOT NULL,
    name      VARCHAR(30) NOT NULL,
    CONSTRAINT fk_trait1 FOREIGN KEY (subject_id) REFERENCES trait (id),
    CONSTRAINT fk_trait2 FOREIGN KEY (object_id) REFERENCES trait (id)
);