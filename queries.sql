-- Para consultar as principais host_locations
select * from host_location order by count desc limit 15;

-- Para consultar a contagem de host_locations distintos
select count (distinct location) from host_location;

-- Para consultar os principais bigramas do nome
select * from bigram order by count desc limit 15;

-- Para consultar os principais unigramas nome
select * from unigram order by count desc limit 15;

-- Para analisar os host_locations mais antigos
select host_location, min(host_since) as min_host_since from host_loc_since group by host_location order by min(host_since) asc limit 5;
