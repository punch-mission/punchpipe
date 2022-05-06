/*
 These commands can be run to set up the Science Reduction Database for punchpipe.
 */

CREATE DATABASE IF NOT EXISTS punchpipe;

USE punchpipe;

CREATE TABLE flows (
    flow_id  CHAR(44) UNIQUE NOT NULL,
    flow_type VARCHAR(64) NOT NULL,
    state VARCHAR(64) NOT NULL,
    creation_time DATETIME NOT NULL,
    start_time DATETIME,
    end_time DATETIME,
    priority INT NOT NULL,
    call_data LONGTEXT,
    PRIMARY KEY ( flow_id )
);

CREATE TABLE files (
    file_id INT UNSIGNED UNIQUE NOT NULL AUTO_INCREMENT,
    level INT NOT NULL,
    file_type CHAR(2) NOT NULL,
    observatory CHAR(1) NOT NULL,
    file_version INT NOT NULL,
    software_version INT NOT NULL,
    date_acquired DATETIME NOT NULL,
    date_obs DATETIME NOT NULL,
    date_end DATETIME NOT NULL,
    polarization CHAR(2),
    state VARCHAR(64) NOT NULL,
    processing_flow CHAR(44) NOT NULL,
    file_name char(29) GENERATED ALWAYS AS
        (concat("L", level ,"_", file_type, observatory, "_",
            DATE_FORMAT(date_acquired, '%Y%m%d%H%i%s'),
            "_", 'v', file_version, '.fits' )),
    PRIMARY KEY ( file_id ),
    FOREIGN KEY ( processing_flow )
                   REFERENCES flows(flow_id)
);

CREATE TABLE relationships (
    id INT UNSIGNED UNIQUE NOT NULL AUTO_INCREMENT,
    parent INT UNSIGNED NOT NULL,
    child INT UNSIGNED NOT NULL,
    FOREIGN KEY (parent)
                           REFERENCES files(file_id),
    FOREIGN KEY (child)
                           REFERENCES  files(file_id)
);