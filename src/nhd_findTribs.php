#!/user/bin/env drush
<?php

$args = array();
while ($arg = drush_shift()) {
  $args[] = $arg;
}

$ftype = 'nhd_full_drainage';
// Is single command line arg?
if (count($args) >= 2) {
  // Do command line, single element settings
  $outlet_hydroid = $args[0];
  $name = $args[1];
  if (isset($args[2])) {
    $ftype = $args[2];
  }
} else {
  error_log("Usage: nhd_findTribs.php outlet_hydroid new_name [ftype='nhd_full_drainage']");
  die;
}

dh_hydro_addMergedFeature($outlet_hydroid, $name, $ftype);

?>