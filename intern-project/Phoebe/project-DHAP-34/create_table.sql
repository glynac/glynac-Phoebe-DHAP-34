CREATE SCHEMA IF NOT EXISTS intern_project;

CREATE TABLE IF NOT EXISTS intern_project.stg_phoebe_users (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    gender VARCHAR(20),
    ip_address VARCHAR(50),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);