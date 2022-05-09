/*
 These commands can be run to completely delete the Science Reduction Database tables.
 */
USE punchpipe;

DROP TABLE IF EXISTS relationships;
DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS flows;
