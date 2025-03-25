SELECT
date_trunc(published_date, month) as publish_month,
regexp_replace(id, r'^\w+-', "https://arxiv.org/abs/") as url ,
JSON_QUERY_ARRAY(authors) as json_authors,
split(regexp_replace(authors, r"\'|\[|\[|\]", ''), ',') as split_authors,
*
FROM {{ source('asyvak', 'week3_arxiv') }}