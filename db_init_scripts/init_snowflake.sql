
CREATE SCHEMA IF NOT EXISTS raw_data_snowflake;

SET search_path TO raw_data_snowflake;

CREATE TABLE IF NOT EXISTS Journal (
    id SERIAL PRIMARY KEY,
    name TEXT,
    journal_ref TEXT
);

CREATE TABLE IF NOT EXISTS Submitter (
    id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    third_name TEXT
);

CREATE TABLE IF NOT EXISTS License (
    id SERIAL PRIMARY KEY,
    name TEXT
);




CREATE TABLE IF NOT EXISTS Publication (
    id SERIAL PRIMARY KEY,
    title TEXT,
    doi TEXT,
    arxiv_id TEXT,  
    update_date DATE,
    comments TEXT,
    submitter_id INT,
    license_id INT,

    FOREIGN KEY (submitter_id) REFERENCES Submitter(id),
    FOREIGN KEY (license_id) REFERENCES License(id)
);

CREATE TABLE IF NOT EXISTS Version (
    id SERIAL PRIMARY KEY,
    publication_id INT,
    name TEXT,
    create_date DATE,

    FOREIGN KEY (publication_id) REFERENCES Publication(id)
);

CREATE TABLE IF NOT EXISTS Journal_specifics (
    id SERIAL PRIMARY KEY,
    journal_id INT,
    publication_id INT,
    FOREIGN KEY (journal_id) REFERENCES Journal(id),
    FOREIGN KEY (publication_id) REFERENCES Publication(id)
);

CREATE TABLE IF NOT EXISTS Person (
    id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    third_name TEXT
);

CREATE TABLE IF NOT EXISTS Authorship (
    id SERIAL PRIMARY KEY,
    author_id INT,
    publication_id INT,
    FOREIGN KEY (author_id) REFERENCES Person(id),
    FOREIGN KEY (publication_id) REFERENCES Publication(id)
);

CREATE TABLE IF NOT EXISTS Sub_category (
    id SERIAL PRIMARY KEY,
    name TEXT
);

CREATE TABLE IF NOT EXISTS Category (
    id SERIAL PRIMARY KEY,
    name TEXT
);

CREATE TABLE IF NOT EXISTS Publication_Category (
    id SERIAL PRIMARY KEY,
    category_id INT,
    subcategory_id INT,
    publication_id INT,
    FOREIGN KEY (category_id) REFERENCES Category(id),
    FOREIGN KEY (subcategory_id) REFERENCES Sub_category(id),
    FOREIGN KEY (publication_id) REFERENCES Publication(id)
);

