<?php
namespace Mongotd;

/*
 * Class takes a list of nids/sids to scan for anomalies using 3 sigma method.
 * It looks at values at the same time period each day for a specified number of days back in time.
 * If the current value differs more than 3 standard deviations from the average at this time of day,
 * the value is considered an anomaly.
 *
 * There is a smoothing interval that can be set so that instead of looking at
 * a given point in time, we look at the average during the interval.
 */
class AnomalyScanner3Sigma{
    /** @var  Connection */
    private $conn;

    /**
     * @param $conn Connection
     */
    public function __construct($conn){
        $this->conn = $conn;
    }

    /**
     * @param $nidsidPairs           array         Ex: [['nid' => $nid, 'sid' => $sid]]
     * @param $datetimeToCheck       \DateTime
     * @param $daysToScan            int
     * @param $smoothIntervalInSecs  int           Average values at dateTimeToCheck and this amount of seconds in the past
     *
     * @return array
     */
    public function scan($nidsidPairs, $datetimeToCheck, $daysToScan = 20, $smoothIntervalInSecs = 300){
        $datetimeToCheck = clone $datetimeToCheck;
        $datetimeToCheck->setTimeZone(new \DateTimeZone('UTC'));

        $secondOfInterest = $datetimeToCheck->getTimestamp()%86400;
        $startdate = clone $datetimeToCheck;
        $enddate = clone $datetimeToCheck;
        $startdate->sub(\DateInterval::createFromDateString($daysToScan . ' days'));
        $startdate->setTime(0, 0, 0);
        $enddate->setTime(0, 0, 0);
        $mongodateStart = new \MongoDate($startdate->getTimestamp());
        $mongodateEnd = new \MongoDate($enddate->getTimestamp());

        $nidsidPairsWithAnomaly = array();
        foreach($nidsidPairs as $nidsidPair){
            $score = $this->getScore($nidsidPair['nid'], $nidsidPair['sid'], $mongodateStart, $mongodateEnd, $secondOfInterest, $smoothIntervalInSecs);
            if($score > 3){
                $nidsidPairsWithAnomaly[] = $nidsidPair;
            }
        }

        return $nidsidPairsWithAnomaly;
    }

    /**
     * @param $nid                  int
     * @param $sid                  int
     * @param $mongodateStart       \MongoDate
     * @param $mongodateEnd         \MongoDate
     * @param $secondOfInterest     int
     * @param $smoothIntervalInSecs int
     *
     * @return float
     */
    private function getScore($nid, $sid, $mongodateStart, $mongodateEnd, $secondOfInterest, $smoothIntervalInSecs){
        // The projection gives the seconds during the day which we want values for
        $projection = array();
        for($i = 0; $i < $smoothIntervalInSecs; $i++){
            if($secondOfInterest - $i < 0){
                $secondOfInterest = 86400 + $i;
            }
            $projection[] = 'vals.' . ($secondOfInterest - $i);
        }

        // Find values for the previous days, around the smoothing interval
        $cursor = $this->conn->col('cv')->find(array(
            'mongodate' => array('$gte' => $mongodateStart, '$lt' => $mongodateEnd),
            'nid' => $nid,
            'sid' => $sid
        ), $projection);

        if($cursor->count() == 0){
            return 0;
        }

        $prevVals = array();
        foreach($cursor as $doc){
            foreach($doc['vals'] as $second => $value){
                $prevVals[] = $value;
            }
        }

        // Find values for today, around the smoothing interval
        $cursor = $this->conn->col('cv')->find(array(
             'mongodate' => $mongodateEnd,
             'nid' => $nid,
             'sid' => $sid
        ), $projection);

        if($cursor->count() == 0){
            return 0;
        }

        $currVals = array();
        foreach($cursor as $doc){
            foreach($doc['vals'] as $second => $value){
                $currVals[] = $value;
            }
        }

        return $this->calculateScore($prevVals, $currVals);
    }

    /**
     * @param $prevVals    number[]
     * @param $currentVals number[]
     *
     * @return float
     */
    private function calculateScore(array $prevVals, array $currentVals){
        $prevStats = $this->calcAvgAndStd($prevVals);
        $currStats = $this->calcAvgAndStd($currentVals);

        if($prevStats['std'] == 0){
            return 0;
        }

        return abs($currStats['avg'] - $prevStats['avg'])/$prevStats['std'];
    }

    /**
     * @param $vals number[]
     *
     * @return array
     */
    private function calcAvgAndStd(array $vals){
        $sum = array_sum($vals);
        $n = count($vals);
        $avg = $sum/$n;
        $var = 0;

        foreach($vals as $val){
            $var += pow($val - $avg, 2);
        }

        $var /= $n;
        $std = sqrt($var);

        return array('avg' => $avg, 'std' => $std);
    }
}