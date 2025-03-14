SELECT 
date_trunc(published_date, month) AS publish_month,
  regexp_replace(id, r'^\w+-', "https://arxiv.org/abs/") as url,

  JSON_QUERY_ARRAY(authors) json_authors,

  split(regexp_replace(authors, r"\'|\[|\]", ''), ',') as split_authors,
*
 FROM {{source('mtalalaievskyi','week3_arxiv')}}