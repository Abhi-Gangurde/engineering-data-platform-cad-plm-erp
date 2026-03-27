CREATE TABLE dbo.assembly_parts (
    assembly_id VARCHAR(10) NOT NULL PRIMARY KEY,
    part_id VARCHAR(10) NOT NULL,
    quantity SMALLINT NOT NULL,
    last_modified DATETIME2(0) NOT NULL
);
