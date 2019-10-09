#!/user/bin/env drush
<?php

$eid = 251517;
$name = 'Chickahominy River';
$ftype = 'nhd_full_drainage';
dh_hydro_addMergedFeature($eid, $name, $ftype);

?>