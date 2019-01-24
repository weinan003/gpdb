/* contrib/file_fdw/file_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alluxio_fdw" to load this file. \quit

CREATE FUNCTION alluxio_fdw_handler()
RETURNS fdw_handler 
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION alluxio_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER alluxio_fdw
  HANDLER alluxio_fdw_handler
  VALIDATOR alluxio_fdw_validator;
