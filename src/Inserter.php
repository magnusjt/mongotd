<?php namespace Mongotd;

class Inserter{

    /** @var  GaugeInserter */
    private $gaugeInserter;

    /** @var  DeltaConverter */
    private $deltaConverter;

    /** @var  AnomalyDetector */
    private $anomalyDetector;

    /** @var bool  */
    private $doAnomalyDetection;

    /** @var CounterValue[] */
    private $cvs = array();

    /** @var CounterValue[] */
    private $cvsIncremental = array();

    public function __construct($gaugeInserter, $deltaConverter, $anomalyDetector, $doAnomalyDetection = false){
        $this->gaugeInserter   = $gaugeInserter;
        $this->deltaConverter  = $deltaConverter;
        $this->anomalyDetector = $anomalyDetector;
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
                $this->anomalyDetector->detect($this->cvs);
            }
        }

        $this->cvs = array();
        $this->cvsIncremental = array();
    }
}