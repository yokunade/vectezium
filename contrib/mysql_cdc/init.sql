-- The image's init client defaults to latin1; make it read this UTF-8 file correctly.
SET NAMES utf8mb4;

-- CDC user with the privileges the mysql_cdc source needs.
CREATE USER 'vector_cdc'@'%' IDENTIFIED BY 'cdcpw';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'vector_cdc'@'%';

CREATE DATABASE inventory;
USE inventory;

-- A table exercising most MySQL types.
CREATE TABLE customers (
  id          INT PRIMARY KEY,
  name        VARCHAR(100) NOT NULL,
  email       VARCHAR(200),
  balance     DECIMAL(12,2),
  ratio       FLOAT,
  score       DOUBLE,
  big         BIGINT UNSIGNED,
  tiny        TINYINT,
  active      BIT(1),
  flags       BIT(12),
  born        DATE,
  wake_up     TIME(3),
  created     DATETIME(6),
  updated     TIMESTAMP(3) NULL,
  yr          YEAR,
  status      ENUM('new','active','closed'),
  tags        SET('a','b','c'),
  profile     JSON,
  avatar      VARBINARY(16),
  bio         TEXT,
  latin       VARCHAR(20) CHARACTER SET latin1,
  location    POINT SRID 4326,
  ssn         CHAR(11)
);

INSERT INTO customers VALUES
  (1, 'Alice', 'alice@example.com', 1234.50, 0.1, 3.14159, 18446744073709551615, -5, b'1', b'101010101010',
   '1990-05-17', '07:30:00.250', '2024-01-01 12:00:00.123456', '2024-01-01 12:00:00.500', 2024,
   'active', 'a,c', '{"tier": "gold", "langs": ["en", "fr"]}', X'DEADBEEF', 'Hello', 'café',
   ST_SRID(POINT(1, 2), 4326), '123-45-6789'),
  (2, 'Bob', NULL, -0.01, NULL, NULL, 42, 0, b'0', b'0', NULL, NULL, NULL, NULL, NULL,
   'new', '', NULL, NULL, NULL, NULL, NULL, NULL),
  (3, 'Carol', 'carol@example.com', 0, 1.5, 2.5, 0, 1, b'1', b'1', '2000-02-29', '-12:00:00', '1999-12-31 23:59:59', NULL, 1999,
   'closed', 'b', '[]', X'00', 'Bye', 'naïve', NULL, NULL);

-- A table that is filtered out by the example configuration.
CREATE TABLE audit_log (
  id   BIGINT AUTO_INCREMENT PRIMARY KEY,
  note VARCHAR(100)
);
INSERT INTO audit_log (note) VALUES ('ignored');
