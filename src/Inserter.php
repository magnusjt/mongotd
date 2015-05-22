<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;
use \DateTime;
use \InvalidArgumentException;

class Inserter{
    /** @var  GaugeInserter */
    private $gaugeInserter;

    /** @var  DeltaConverter */
    private $deltaConverter;

    /** @var  AnomalyScannerInterface */
    private $anomalyScanner = null;

    /** @var CounterValue[] */
    private $cvs = array();

    /** @var CounterValue[] */
    private $cvsIncremental = array();

    public function __construct(GaugeInserter $gaugeInserter, DeltaConverter $deltaConverter, LoggerInterface $logger){
        $this->logger = $logger;
        $this->gaugeInserter   = $gaugeInserter;
        $this->deltaConverter  = $deltaConverter;

        $this->cvs = array();
        $this->cvsIncremental = array();
    }

    public function setAnomalyScanner(AnomalyScannerInterface $anomalyScanner){
        $this->anomalyScanner = $anomalyScanner;
    }

    /**
     * @param int $intervalInSeconds How often you plan to insert data. Affects preallocation of space and delta-calc scaling.
     */
    public function setInterval($intervalInSeconds = 300){
        $this->deltaConverter->setInterval($intervalInSeconds);
        $this->gaugeInserter->setInterval($intervalInSeconds);
    }

    /**
     * @param $sid           string
     * @param $nid           string
     * @param $datetime      DateTime
     * @param $value         number
     * @param $isIncremental bool
     */
    public function add($sid, $nid, DateTime $datetime, $value, $isIncremental = false){
        if(!is_numeric($value)){
            throw new InvalidArgumentException('Value should be numeric');
        }

        if(is_string($value)){
            if(ctype_digit($value)){
                $value = (int)$value;
            }else{
                $value = (float)$value;
            }
        }

        $cv = new CounterValue($sid, $nid, clone $datetime, $value);

        if($isIncremental){
            $this->cvsIncremental[] = $cv;
        }else{
            $this->cvs[] = $cv;
        }
    }

    public function insert(){
        if(count($this->cvsIncremental) > 0){
            $cvs = $this->deltaConverter->convert($this->cvsIncremental);
            $this->cvs = array_merge($this->cvs, $cvs);
        }

        if(count($this->cvs) > 0){
            $this->gaugeInserter->insert($this->cvs);
            if($this->anomalyScanner !== null){
                $this->anomalyScanner->scan($this->cvs);
            }
        }

        $this->cvs = array();
        $this->cvsIncremental = array();
    }
}