<?php namespace Mongotd;

use Psr\Log\LoggerInterface;

class GaugeInserter{
    /** @var  Connection */
    private $conn;

    /** @var  LoggerInterface */
    private $logger;

    /** @var int */
    private $interval;

    private $secondsInDay = 86400;

    public function __construct($conn, $interval = 300, LoggerInterface $logger = NULL){
        $this->conn       = $conn;
        $this->logger     = $logger;
        $this->interval = $interval;
    }

    /**
     * @param $cvs CounterValue[]
     */
    public function addBatch($cvs){
        $col = $this->conn->col('cv');
        $batchUpdate = new \MongoUpdateBatch($col);

        foreach($cvs as $cv){
            $mongodate = new \MongoDate(DateTimeHelper::clampToDay($cv->datetime)->getTimestamp());
            $this->preAllocateIfNecessary($col, $cv->sid, $cv->nid, $mongodate);

            $seconds = $cv->datetime->getTimestamp()%$this->secondsInDay;
            $batchUpdate->add(array(
                'q' => array('sid' => $cv->sid, 'nid' => $cv->nid, 'mongodate' => $mongodate),
                'u' => array('$set' => array('vals.' . $seconds => $cv->value))
            ));
        }

        $batchUpdate->execute(array('w' => 1));
    }

    /**
     * @param $col \MongoCollection
     * @param $sid int
     * @param $nid int
     * @param $mongodate \MongoDate
     */
    private function preAllocateIfNecessary($col, $sid, $nid, $mongodate){
        if($col->find(array('sid' => $sid, 'nid' => $nid, 'mongodate' => $mongodate), array('sid' => 1))->limit(1)->count() == 0){
            $valsPerSec = array();
            for($second = 0; $second < $this->secondsInDay; $second += $this->interval){
                $valsPerSec[$second] = null;
            }

            $col->insert(array(
                             'sid'       => $sid,
                             'nid'       => $nid,
                             'mongodate' => $mongodate,
                             'vals'      => $valsPerSec,
                         ));

        }
    }
}