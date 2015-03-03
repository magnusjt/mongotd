<?php
namespace Mongotd;

/*
 * Scan for anomalies using kolmogorov-smirnov test
 * Check windows of data within smoothing window for a number of days in the past.
 * If the ks distance is large, or pValue is low, it means there is an anomaly
 */
class AnomalyScannerKs extends AnomalyScanner{
    /** @var int  */
    private $daysToScan = 20;

    /** @var int  */
    private $windowLengthInSeconds = 7200;

    /** @var float  */
    private $dTreshold = 0.5;

    /** @var float  */
    private $pTreshold = 0.01;

    /**
     * @param $cvs      CounterValue[]
     * @param $datetime \DateTime
     *
     * @return array
     */
    public function scan($cvs, \DateTime $datetime){
        $datetimeToCheck = clone $datetime;
        $datetimeToCheck->setTimeZone(new \DateTimeZone('UTC'));
        $windowEndPosition = $datetimeToCheck->getTimestamp()%86400;

        $datetimeToCheck->setTime(0, 0, 0);
        $controlStartDate = clone $datetimeToCheck;
        $controlEndDate = clone $datetimeToCheck;
        $controlStartDate->sub(\DateInterval::createFromDateString($this->daysToScan . ' days'));
        $controlEndDate->sub(\DateInterval::createFromDateString('1 day'));

        $controlMongodateStart = new \MongoDate($controlStartDate->getTimestamp());
        $controlMongodateEnd = new \MongoDate($controlEndDate->getTimestamp());
        $mongodateToCheck = new \MongoDate($datetimeToCheck->getTimestamp());

        foreach($cvs as $cv){
            $prevDataPoints = $this->getValsWithinWindow($cv->nid, $cv->sid, $controlMongodateStart, $controlMongodateEnd, $this->windowLengthInSeconds, $windowEndPosition);
            $currDataPoints = $this->getValsWithinWindow($cv->nid, $cv->sid, $mongodateToCheck, $mongodateToCheck, $this->windowLengthInSeconds, $windowEndPosition);
            $ks = $this->calculateKsTwoSided($prevDataPoints, $currDataPoints);

            if($ks !== false and $ks['p'] < $this->pTreshold and $ks['d'] > $this->dTreshold){
                $predicted = array_sum($prevDataPoints)/count($prevDataPoints);
                $this->storeAnomaly($cv->nid, $cv->sid, $datetime, $predicted, $cv->value);
            }
        }
    }

    public function calculateKsTwoSided(array $data1, array $data2){
        $n1 = count($data1);
        $n2 = count($data2);

        if($n1 == 0 or $n2 == 0){
            return false;
        }

        sort($data1);
        sort($data2);

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