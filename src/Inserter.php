<?php namespace Mongotd;

class Inserter{
    /** @var  GaugeInserter */
    private $gaugeInserter;

    /** @var  DeltaConverter */
    private $deltaConverter;

    /** @var  AnomalyScanner */
    private $anomalyScanner;

    /** @var bool  */
    private $doAnomalyDetection;

    /** @var CounterValue[] */
    private $cvs = array();

    /** @var CounterValue[] */
    private $cvsIncremental = array();

    /**
     * @param $gaugeInserter      GaugeInserter
     * @param $deltaConverter     DeltaConverter
     * @param $doAnomalyDetection bool
     * @param $anomalyScanner     AnomalyScanner
     */
    public function __construct($gaugeInserter, $deltaConverter, $doAnomalyDetection = false, $anomalyScanner = null){
        $this->gaugeInserter   = $gaugeInserter;
        $this->deltaConverter  = $deltaConverter;
        $this->anomalyScanner  = $anomalyScanner;
        $this->doAnomalyDetection = $doAnomalyDetection;

        $this->cvs = array();
        $this->cvsIncremental = array();
    }

    /**
     * @param $sid           int|string
     * @param $nid           int|string
     * @param $datetime      \DateTime
     * @param $value         number
     * @param $isIncremental bool
     */
    public function add($sid, $nid, $datetime, $value, $isIncremental = false){
        if(!is_numeric($value)){
            throw new \InvalidArgumentException('Value should be numeric');
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
            if($this->doAnomalyDetection){
                $this->anomalyScanner->scan($this->cvs);
            }
        }

        $this->cvs = array();
        $this->cvsIncremental = array();
    }
}