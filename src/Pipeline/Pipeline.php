<?php namespace Mongotd\Pipeline;

class Pipeline{
    /**
     * @param $sequence array
     * @param $output   null|Series|Series[]|mixed
     *
     * @return Series|Series[]|mixed
     */
    public function run(array $sequence, $output = null){
        foreach($sequence as $subSequence){
            if(is_array($subSequence)){
                $subOutputs = [];
                foreach($subSequence as $subItem){
                    $subOutputs[] = $this->run($subItem, $output);
                }
                $output = $subOutputs;
            }else{
                $output = $subSequence->run($output);
            }
        }

        return $output;
    }
}