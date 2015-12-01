<?php namespace Mongotd\Pipeline;

use DateTime;
use DateTimeZone;
use MongoDate;
use Mongotd\Anomaly;
use Mongotd\Connection;
use Mongotd\CounterValue;

class FindAnomalies{
    public $conn;
    public $matchObject;

    public function __construct(Connection $conn, DateTime $start, DateTime $end, array $nids = array(), array $sids = array(), $minCount = 1, $limit = 20){
        $this->conn = $conn;

        foreach($nids as &$nid){
            $nid = (string)$nid;
        }

        foreach($sids as &$sid){
            $sid = (string)$sid;
        }

        // First match with datetime and nid/sid we are interested in
        $matchInitial = array();
        if(count($nids) > 0){
            $matchInitial['nid'] = ['$in' => $nids];
        }

        if(count($sids) > 0){
            $matchInitial['sid'] = ['$in' => $sids];
        }

        $matchInitial['mongodate'] = [
            '$gte' => new MongoDate($start->getTimestamp()),
            '$lte' => new MongoDate($end->getTimestamp())
        ];

        // Next ensure that everything is sorted by date in ascending order
        $sortOnDate = ['mongodate' => 1];

        // Next group by nid/sid, and count the number of anomalies for each such combo
        $groupByNidSidAndDoCount = [
            '_id'       => ['nid' => '$nid', 'sid' => '$sid'], // This is what is aggregated on
            'count'     => ['$sum' => 1],
            'anomalies' => ['$push' => ['mongodate' => '$mongodate', 'predicted' => '$predicted', 'actual' => '$actual']],
            'sid'       => ['$first' => '$sid'],
            'nid'       => ['$first' => '$nid'],
        ];

        // Now grab only the combos with a minimum number of anomalies
        $matchCountHigherThanMin = ['count' => ['$gte' => $minCount]];

        // Next sort on count in descending order, so the most anomalies appear first in the result
        $sortOnCountDesc = ['count' => -1];

        // Lastly project only the items we are interested in
        $project = [
            '_id' => 0,
            'sid' => 1,
            'nid' => 1,
            'count' => 1,
            'anomalies' => 1
        ];

        $this->matchObject = [
            ['$match'   => $matchInitial],
            ['$sort'    => $sortOnDate],
            ['$group'   => $groupByNidSidAndDoCount],
            ['$match'   => $matchCountHigherThanMin],
            ['$sort'    => $sortOnCountDesc],
            ['$limit'   => $limit],
            ['$project' => $project]
        ];
    }

    public function run(){
        $res = $this->conn->col('anomalies')->aggregate($this->matchObject);
        $timezone = new DateTimeZone(date_default_timezone_get());

        $result = [];
        foreach($res['result'] as $doc){
            $nid = $doc['nid'];
            $sid = $doc['sid'];
            $count = $doc['count'];

            $anomalies = [];
            foreach($doc['anomalies'] as $subdoc){
                $timestamp = $subdoc['mongodate']->sec;
                $datetime = new DateTime('@'.$timestamp);
                $datetime->setTimezone($timezone);
                $cv = new CounterValue($sid, $nid, $datetime, $subdoc['actual']);
                $anomalies[] = new Anomaly($cv, $subdoc['predicted']);
            }

            $result[] = array(
                'nid' => $nid,
                'sid' => $sid,
                'count' => $count,
                'anomalies' => $anomalies
            );
        }

        return $result;
    }
}