<?php namespace Mongotd;

use Psr\Log\LoggerInterface;

class GaugeInserter{
    /** @var  Connection */
    private $conn;

    /** @var  LoggerInterface */
    private $logger;

    /** @var int  */
    private $resolution;

    public function __construct($conn, $resolution, LoggerInterface $logger = NULL){
        $this->conn       = $conn;
        $this->logger     = $logger;
        $this->resolution = $resolution;

        if($this->resolution > Resolution::HOUR){
            $this->resolution = Resolution::HOUR;
        }
    }

    /**
     * @param $cvs CounterValue[]
     */
    public function addBatch($cvs){
        $col            = $this->conn->col('cv');
        $batch_inserter = new \MongoInsertBatch($col);
        $batch_updater  = new \MongoUpdateBatch($col);

        $inserts_needed = false;
        foreach($cvs as $cv){
            $datetime = clone $cv->datetime;
            $hour   = (int)$datetime->format('H');
            $minute = (int)$datetime->format('i');
            $minute -= $minute%$this->resolution; // Clamp minute to nearest minute with resolution

            // A new doc is stored on a daily basis, so find a unique date value for this day
            $datetime->setTime(0,0,0);
            $mongodate = new \MongoDate($datetime->getTimestamp());

            $batch_updater->add(array(
                'q' => array('sid' => $cv->sid, 'nid' => $cv->nid, 'mongodate' => $mongodate),
                'u' => array('$set' => array('hours.' . $hour . '.' . $minute => $cv->value))
            ));

            $doc = $col->findOne(array('sid' => $cv->sid, 'nid' => $cv->nid, 'mongodate' => $mongodate), array('sid' => 1));
            if(!$doc){
                $batch_inserter->add($this->getEmptyDoc($cv->sid, $cv->nid, $mongodate));
                $inserts_needed = true;
            }
        }

        if($inserts_needed){
            $batch_inserter->execute(array('w' => 1));
        }

        $batch_updater->execute(array('w' => 1));
    }

    /**
     * @param $sid int|string
     * @param $nid int|string
     * @param $mongodate \MongoDate
     *
     * @return array
     */
    private function getEmptyDoc($sid, $nid, $mongodate){
        $minutes = array();
        for($minute = 0; $minute < 60; $minute += $this->resolution){
            $minutes[$minute] = false;
        }

        $hours = array();
        for($hour = 0; $hour < 24; $hour++){
            $hours[$hour] = $minutes;
        }

        return array(
            'sid'       => $sid,
            'nid'       => $nid,
            'mongodate' => $mongodate,
            'hours'     => $hours,
        );
    }
}