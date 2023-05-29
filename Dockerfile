FROM postgres:15.3
COPY ./init-events-db.sql /docker-entrypoint-initdb.d/init-events-db.sql
