/*
 These commands can be run to completely delete the Science Reduction Database tables.
 */
USE punchpipe;

DROP TABLE IF EXISTS relationships;
DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS flows;
DROP TABLE IF EXISTS packets;
DROP TABLE IF EXISTS sci_packets;
DROP TABLE IF EXISTS eng_packets;
