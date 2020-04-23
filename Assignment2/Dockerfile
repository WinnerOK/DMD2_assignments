FROM postgres:12-alpine

COPY ./dvdrental-database/postgree /backup
COPY ./dvdrental-database/postgree/restore.sql /docker-entrypoint-initdb.d/1-restore.sql