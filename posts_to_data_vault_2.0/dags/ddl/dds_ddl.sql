-- Create table hub_user
DROP TABLE IF EXISTS dds.hub_user CASCADE;
CREATE TABLE IF NOT EXISTS dds.hub_user (
  user_id INT PRIMARY KEY,
  user_hash_key CHAR(32) NOT NULL,
  load_date TIMESTAMP,
  source_system VARCHAR(50),
  unique (user_hash_key)
);

-- Create table hub_post
DROP TABLE IF EXISTS dds.hub_post CASCADE;
CREATE TABLE dds.hub_post (
  post_id INT PRIMARY KEY,
  post_hash_key CHAR(32) NOT NULL,
  load_date TIMESTAMP,
  source_system VARCHAR(50),
  unique (post_hash_key)
);

-- Create table link users_posts
DROP TABLE IF EXISTS dds.link_user_post CASCADE;
CREATE TABLE IF NOT EXISTS dds.link_user_post (
  user_id INT NOT NULL,
  post_id INT NOT NULL,
  user_post_hash_key CHAR(32) NOT NULL,
  load_date TIMESTAMP,
  source_system VARCHAR(50),
  PRIMARY KEY (user_id, post_id),
  unique (user_post_hash_key)
);

-- Create table satellite for posts
DROP TABLE IF EXISTS dds.satellite_post CASCADE;
CREATE TABLE IF NOT EXISTS dds.satellite_post (
  post_hash_key CHAR(32) NOT NULL,
  title VARCHAR(255) NOT NULL,
  body TEXT NOT NULL,
  load_date TIMESTAMP,
  source_system VARCHAR(50),
  post_hash_diff CHAR(32),
  FOREIGN KEY (post_hash_key) REFERENCES dds.hub_post (post_hash_key)
);
