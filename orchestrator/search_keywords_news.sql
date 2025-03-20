CREATE SCHEMA news;
CREATE TABLE news.search_keywords(
    keyword_id VARCHAR(255),
    keyword VARCHAR(255),
    num_records INT,
    country VARCHAR(10),
    data_owner VARCHAR(255),
    topic VARCHAR(255),
    domain VARCHAR(255),
    domain_exact VARCHAR(255),
    theme VARCHAR(255),
    near VARCHAR(255),
    repeat_ VARCHAR(255)
);

-- example keyword
-- insert into news.search_keywords values(1,'climate change',250,'uk','FBK',null,null,null,null,null)