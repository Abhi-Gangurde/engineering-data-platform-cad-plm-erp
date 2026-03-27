CREATE TABLE dbo.plm_revisions (
    part_id VARCHAR(10) NOT NULL,
    revision VARCHAR(5) NOT NULL,
    status VARCHAR(20) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE NULL,
    last_modified DATETIME2(0) NOT NULL,
    CONSTRAINT PK_part_revisions PRIMARY KEY (part_id, revision)
);
