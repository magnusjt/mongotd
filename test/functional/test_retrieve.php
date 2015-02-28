<?php
require(__DIR__.'/../../vendor/autoload.php');
date_default_timezone_set('Europe/Oslo');

$conn = new \Mongotd\Connection('localhost', 'mongotdtest', 'mongotdtest');
$mongotd = new \Mongotd\Mongotd($conn, null);

$retriever = $mongotd->getRetriever();
$start = microtime(true);
$valsByDate = $retriever->get(1, 1, new DateTime("2014-09-10 00:00:00"), new DateTime('2014-09-20 00:00:00'), \Mongotd\Resolution::DAY, \Mongotd\Aggregation::SUM);
echo "Time taken: " . (microtime(true) - $start) . "\n";