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

