<?php
namespace Mongotd;

/*
 * Scan for anomalies using kolmogorov-smirnov test
 * Check windows of data within smoothing window for a number of days in the past.
 * If the ks distance is large, it means there is an anomaly
 */
class AnomalyScannerKs{
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
    public function scan($nidsidPairs, $datetimeToCheck, $daysToScan = 20, $smoothIntervalInSecs = 7200){
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
            $ks = $this->getKs($nidsidPair['nid'], $nidsidPair['sid'], $mongodateStart, $mongodateEnd, $secondOfInterest, $smoothIntervalInSecs);
            if($ks !== false and $ks['p'] < 0.01 and $ks['d'] > 0.5){
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
    private function getKs($nid, $sid, $mongodateStart, $mongodateEnd, $secondOfInterest, $smoothIntervalInSecs){
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

        $prevVals = array();
        foreach($cursor as $doc){
            foreach($doc['vals'] as $second => $value){
                if($value !== null){
                    $prevVals[] = $value;
                }
            }
        }

        if(count($prevVals) == 0){
            return false;
        }

        // Find values for today, around the smoothing interval
        $cursor = $this->conn->col('cv')->find(array(
                                                   'mongodate' => $mongodateEnd,
                                                   'nid' => $nid,
                                                   'sid' => $sid
                                               ), $projection);

        $currVals = array();
        foreach($cursor as $doc){
            foreach($doc['vals'] as $second => $value){
                if($value !== null){
                    $currVals[] = $value;
                }
            }
        }

        if(count($currVals) == 0){
            return false;
        }

        return $this->twoSided($currVals, $prevVals);
    }

    public function twoSided(array $data1, array $data2){
        sort($data1);
        sort($data2);

        $n1 = count($data1);
        $n2 = count($data2);

        $data_all = array_merge($data1, $data2);

        $cdf1 = array_map(function($elem) use ($n1) {
            return $elem/$n1;
        }, $this->searchSorted($data1, $data_all));

        $cdf2 = array_map(function($elem) use ($n2) {
            return $elem/$n2;
        }, $this->searchSorted($data2, $data_all));

        $largestDiff = max(array_map(function($elem1, $elem2){
            return abs($elem1 - $elem2);
        }, $cdf1, $cdf2));

        $pValue = $this->kolmogorovDistribution(sqrt(($n1+$n2)/($n1*$n2))*$largestDiff);

        return array('d' => $largestDiff, 'p' => $pValue);
    }

    /*
     * Find the index in array a where elements of v can be inserted
     * so that the sort order of a is preserved. Start looking from
     * the right side of a.
     */
    private function searchSorted($arrInsertInto, $arrToBeInserted){
        $res = array();

        for($i = 0; $i < count($arrToBeInserted); $i++){
            $pos = 0;
            for($j = count($arrInsertInto)-1; $j > 0; $j--){
                if($arrToBeInserted[$i] <= $arrInsertInto[$j]){
                    $pos = $j;
                    break;
                }
            }

            $res[] = $pos;
        }

        return $res;
    }

    /*
     * Estimate kolmogorov distribution for $x.
     * Only an estimate since the iteration should really go on forever.
     */
    private function kolmogorovDistribution($x){
        if($x < 0.000001){
            return 1;
        }

        $xPow2Times8 = 8*pow($x, 2);
        $piPow2 = pow(M_PI, 2);
        $factor = $piPow2/$xPow2Times8;

        $sum = 0;
        for ($i = 1; $i < 8; $i++){
            $sum += exp(-pow(2*$i-1, 2)*$factor);
        }

        return sqrt(2 * M_PI) * $sum / $x;
    }
}