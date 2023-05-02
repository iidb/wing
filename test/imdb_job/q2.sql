SELECT count(*)
FROM aka_name AS an,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     info_type AS it,
     movie_companies AS mc,
     movie_info AS mi,
     name AS n,
     role_type AS rt,
     title AS t 
WHERE cn.country_code < 'c'
  and it.info < 'd'
  and n.gender = 1
  and rt.role < 'c'
  and mc.note < 'USA'
  and mi.info < 'JAPAN'
  and t.production_year >= 2007
  and t.production_year < 2015
  and t.title < 'KungFuPanda'
  and t.id = mi.movie_id
  and t.id = mc.movie_id
  and t.id = ci.movie_id
  and mc.movie_id = ci.movie_id
  and mc.movie_id = mi.movie_id
  and mi.movie_id = ci.movie_id
  and cn.id = mc.company_id
  and it.id = mi.info_type_id
  and n.id = ci.person_id
  and rt.id = ci.role_id
  and n.id = an.person_id
  and ci.person_id = an.person_id
  and chn.id = ci.person_role_id;