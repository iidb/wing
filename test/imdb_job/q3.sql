SELECT count(*) 
FROM cast_info AS ci,
     info_type AS it1,
     info_type AS it2,
     movie_info AS mi,
     movie_info AS mi_idx,
     name AS n,
     title AS t 
WHERE ci.note < 'a'
  and it1.info < 'c'
  and it2.info < 'c'
  and mi.info < 'H'
  and mi_idx.info > '8.0'
  and n.gender = 0
  and t.production_year >= 1958
  and t.id = mi.movie_id
  and t.id = mi_idx.movie_id
  and t.id = ci.movie_id
  and ci.movie_id = mi.movie_id
  and ci.movie_id = mi_idx.movie_id
  and mi.movie_id = mi_idx.movie_id
  and n.id = ci.person_id
  and it1.id = mi.info_type_id
  and it2.id = mi_idx.info_type_id;