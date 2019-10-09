copy (
select 
  huc10.hydroid as huc10_tonode_hydroid, huc10.hydrocode AS huc10_tonode_hydrocode, 
  huc10.hydroid, huc10.hydrocode,
  huc12.hydroid as huc12_hydroid, huc12.hydrocode as huc12_hydrocode,
  next_up_huc12.hydroid as next_huc12_hydroid, next_up_huc12.hydrocode as next_huc12_hydrocode,
  next_up_huc10.hydroid as next_huc10_hydroid, next_up_huc10.hydrocode AS next_huc10_hydrocode
from 
  dh_feature as huc10 
left outer join field_data_dh_geofield as huc10_geo on (
  huc10_geo.entity_id = huc10.hydroid 
  and entity_type = 'dh_feature'
)
left outer join field_data_dh_geofield as huc12_geo on (
  st_contains(huc10_geo.dh_geofield_geom, st_pointonsurface(huc12_geo.dh_geofield_geom)) 
  and huc10_geo.dh_geofield_geom && huc12_geo.dh_geofield_geom
  and huc12_geo.entity_type = 'dh_feature'
)
left outer join dh_feature as huc12 on (
  huc12.hydroid = huc12_geo.entity_id 
  and huc12_geo.entity_type = 'dh_feature'
)
left outer join field_data_dh_nextdown_id as next_up_link on (
  next_up_link.dh_nextdown_id_target_id = huc12.hydroid
)
left outer join dh_feature as next_up_huc12 on (
  next_up_huc12.hydroid = next_up_link.entity_id
)
left outer join field_data_dh_geofield as next_up_huc12_geo on (
  next_up_link.entity_id = next_up_huc12_geo.entity_id
  and next_up_link.entity_type = 'dh_feature'
)
left outer join field_data_dh_geofield as next_up_huc10_geo on (
  st_contains(next_up_huc10_geo.dh_geofield_geom, st_pointonsurface(next_up_huc12_geo.dh_geofield_geom)) 
  and next_up_huc10_geo.dh_geofield_geom && next_up_huc12_geo.dh_geofield_geom 
  and next_up_huc10_geo.entity_type = 'dh_feature'
)
left outer join dh_feature as next_up_huc10 on (
  next_up_huc10.hydroid = next_up_huc10_geo.entity_id
)
where 
  -- huc10.hydrocode = '0207000804' 
   huc12.ftype = 'nhd_huc12' 
  and huc10.ftype = 'nhd_huc10' 
  and next_up_huc12.ftype = 'nhd_huc12' 
  and next_up_huc10.ftype = 'nhd_huc10' 
  and next_up_huc10.hydroid <> huc10.hydroid 
  --limit 10 

) to '/tmp/tonode_huc10s.txt'
  ;

-- insert huc 10 linkages (derived in huc10_to_node.sql)

create temp table tmp_huc10_to_node (
  to_hydroid integer,
  to_hydrocode varchar,
  huc10_hydroid integer,
  huc10_hydrocode varchar,
  huc12_hydroid integer,
  huc12_hydrocode varchar,
  next_huc12_hydroid integer,
  next_huc12_hydrocode varchar,
  from_hydroid integer,
  from_hydrocode varchar
);

copy tmp_huc10_to_node from '/tmp/tonode_huc10s.txt' with delimiter E'\t';

insert into field_data_dh_nextdown_id (
  entity_type, 
  bundle, 
  deleted, 
  entity_id, 
  revision_id, 
  language, 
  delta, 
  dh_nextdown_id_target_id
) 

select 'dh_feature', 
  'watershed',
  0,
  links.from_hydroid,  
  links.from_hydroid,
  'und',
  0,
  links.to_hydroid
from  (
  select from_hydroid, to_hydroid 
  from tmp_huc10_to_node 
  group by from_hydroid, to_hydroid 
) as links
left outer join field_data_dh_nextdown_id on ( 
  entity_id = from_hydroid 
  and dh_nextdown_id_target_id = to_hydroid
) 
where field_data_dh_nextdown_id.dh_nextdown_id_target_id is null 
group by links.from_hydroid, links.to_hydroid 
order by links.to_hydroid 
;


-- find watersheds without tribs, but WITH downstream links (to avoid those that are missing in the west)
insert into dh_properties (featureid, entity_type, propvalue, varid, bundle, propname) 
select entity_id, 'dh_feature' as entity_type, 1 as propvalue, v.hydroid, 'dh_properties', v.varname 
from field_data_dh_nextdown_id 
left outer join dh_variabledefinition as v 
on (varkey = 'isheadwater')
left outer join dh_properties as c 
on (c.varid = v.hydroid and c.featureid = entity_id) 
where entity_id not in (
  select dh_nextdown_id_target_id 
  from field_data_dh_nextdown_id 
  where entity_id in (
    select hydroid from dh_feature where bundle = 'watershed' and ftype = 'nhd_huc10'
  )
)
  and field_data_dh_nextdown_id.entity_type = 'dh_feature'
  and field_data_dh_nextdown_id.entity_id in (
    select hydroid from dh_feature where bundle = 'watershed' and ftype = 'nhd_huc10'
  )
  and c.pid is null 
;

INSERT INTO dh_properties_revision (pid, varid, propname, propcode, propvalue, startdate, enddate, featureid, entity_type, bundle)
SELECT a.pid, a.varid, a.propname, a.propcode, a.propvalue, a.startdate, a.enddate, a.featureid, a.entity_type, a.bundle
FROM dh_properties as a
left outer join dh_properties_revision as b 
on (a.pid = b.pid) 
left outer join dh_variabledefinition as c 
on (varkey = 'isheadwater' and c.hydroid = a.varid)
WHERE b.pid IS NULL
and c.hydroid is not null;
