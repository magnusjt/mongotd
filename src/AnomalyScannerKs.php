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
    private $dTreshold = 0.4;

    /** @var float  */
    private $pTreshold = 0.001;

    /**
     * @param $cvs CounterValue[]
     *
     * @return array
     */
    public function scan($cvs){
        foreach($cvs as $cv){
            $controlEndDate = clone $cv->datetime;
            $controlEndDate->sub(\DateInterval::createFromDateString('1 day'));
            $controlStartDate = clone $controlEndDate;
            $controlStartDate->sub(\DateInterval::createFromDateString($this->daysToScan . ' days'));
            $windowEndPosition = $cv->datetime->getTimestamp()%86400;

            $prevDataPoints = $this->getValsWithinWindows($cv->nid, $cv->sid, $controlStartDate, $controlEndDate, $this->windowLengthInSeconds, $windowEndPosition);

            // Prevent anomaly detection when there is little to no data
            if(count($prevDataPoints) < $this->daysToScan){
                continue;
            }

            $currDataPoints = $this->getValsWithinWindows($cv->nid, $cv->sid, $cv->datetime, $cv->datetime, $this->windowLengthInSeconds, $windowEndPosition);
            $ks = $this->calculateKsTwoSided($prevDataPoints, $currDataPoints);

            if($ks !== false and $ks['p'] < $this->pTreshold and $ks['d'] > $this->dTreshold){
                $predicted = array_sum($prevDataPoints)/count($prevDataPoints); // Not really a good prediction, but KS doesn't deal in predictions
                $this->storeAnomaly($cv->nid, $cv->sid, $cv->datetime, $predicted, $cv->value);
            }
        }
    }

    /* The algorithm for this function:
     *
     * - Calculate CDFs for both data series. Do this by creating a set of values concatenated between them
     *   as F-values (CDF(F) = number of values less than F. Sort the arrays, and see where the values from the concatenated
     *   array has to be inserted to keep the order of the array we pretend to insert into.
     *
     * - Take the largest absolute difference between the CDFs, the distance.
     * - Get the p-Value by using the kolmogorov distribution. The p value is the level
     *   at which we cannot reject the hypothesis that the two sample-sets are drawn from different distributions.
     *   I.e: A low p values tells us that there is a bigger chance that the samples where drawn from different distributions.
     *
     */
    public function calculateKsTwoSided(array $data1, array $data2){
        $n1 = count($data1);
        $n2 = count($data2);

        if($n1 == 0 or $n2 == 0){
            return false;
        }

        sort($data1);
        sort($data2);

        /*
         * Concatenate the arrays to get a common list of values to measure against.
         * Remember, CDF is defined as 1/n * sum(number of values less than F), for any F.
         * The concatenated array here gives us common 'F's to work with, so the cdfs
         * are easy to compare.
         */
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
     * the right side of a. Could do bin search here, but not much improvement to be had I think.
     */
    private function searchSorted($arrInsertInto, $arrToBeInserted){
        $res = array();

        for($i = 0; $i < count($arrToBeInserted); $i++){
            $pos = 0;
            for($j = count($arrInsertInto)-1; $j > 0; $j--){
                if($arrToBeInserted[$i] >= $arrInsertInto[$j]){
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