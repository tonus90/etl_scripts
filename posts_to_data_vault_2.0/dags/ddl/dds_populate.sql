-- Procedure to populate the user hub
CREATE OR REPLACE FUNCTION dds.populate_hub()
RETURNS VOID AS $$
BEGIN
  INSERT INTO dds.hub_user (user_id, user_hash_key, load_date, source_system)
  SELECT DISTINCT userid, MD5(userid::TEXT), load_date, source_system
  FROM stg.post
  WHERE userid NOT IN (SELECT user_id FROM dds.hub_user);
  INSERT INTO dds.hub_post (post_id, post_hash_key, load_date, source_system)
  SELECT id, MD5(id::TEXT), load_date, source_system
  FROM stg.post
  WHERE id NOT IN (SELECT post_id FROM dds.hub_post);
END;
$$ LANGUAGE plpgsql;

-- Call procedure
select dds.populate_hub();

-- Procedure to populate the user_post link
CREATE OR REPLACE FUNCTION dds.populate_user_post_link()
RETURNS VOID AS $$
BEGIN
  INSERT INTO dds.link_user_post (user_post_hash_key, user_id, post_id, load_date, source_system)
  SELECT MD5(ROW(userid, id)::TEXT), userid, id, load_date, source_system
  FROM stg.post
  WHERE userid NOT IN (SELECT user_id FROM dds.hub_user) AND 
        id not in (SELECT post_id FROM dds.hub_post);
END;
$$ LANGUAGE plpgsql;

-- Call procedure
select dds.populate_user_post_link();

-- Procedure to populate the post satellite
CREATE OR REPLACE FUNCTION dds.populate_post_satellite()
RETURNS VOID AS $$
BEGIN
  INSERT INTO dds.satellite_post (post_hash_key, title, body, load_date, source_system, post_hash_diff)
  SELECT MD5(id::TEXT), title, body, load_date, source_system, 
            MD5(ROW(title, body, load_date, source_system)::TEXT)
  FROM stg.post;
END;
$$ LANGUAGE plpgsql;

-- Call procedure
select dds.populate_post_satellite();

