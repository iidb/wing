SELECT count(*) 
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     info_type AS it2,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info AS mi_idx,
     title AS t 
WHERE cn.country_code < 'c'
  and ct.kind < 'e'
  and it1.info >= 'genres'
  and it2.info <= 'rating'
  and mi.info < 'c'
  and mi_idx.info > '7.0'
  and t.production_year <= 2010
  and t.production_year >= 2000
  and t.id = mi.movie_id
  and t.id = mi_idx.movie_id
  and mi.info_type_id = it1.id
  and mi_idx.info_type_id = it2.id
  and t.id = mc.movie_id
  and ct.id = mc.company_type_id
  and cn.id = mc.company_id
  and mc.movie_id = mi.movie_id
  and mc.movie_id = mi_idx.movie_id
  and mi.movie_id = mi_idx.movie_id;