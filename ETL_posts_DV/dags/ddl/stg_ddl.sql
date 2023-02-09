CREATE TABLE IF NOT EXISTS stg.post (
    id INT,
    userid INT,
    title VARCHAR(255),
    body TEXT,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'https://jsonplaceholder.typicode.com/posts'
)