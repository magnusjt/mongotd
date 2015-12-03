<?php namespace Mongotd\Pipeline;

/**
 * This class runs a sequence of pipes.
 * It takes the output from the previous item
 * and sends it to the next item.
 *
 * Each item in a sequence may be a list of sequences.
 * In this case, the output of the previous item
 * will be given as a start value to each of the
 * sequences in the list.
 */
class Pipeline{
    /**
     * @param $sequence array
     * @param $output   null|Series|Series[]|mixed
     *
     * @return Series|Series[]|mixed
     */
    public function run(array $sequence, $output = null){
        foreach($sequence as $sequenceList){
            if(is_array($sequenceList)){
                // $sequenceList is in fact a list of sequences
                // Run each sequence with the same input.
                $subOutputs = [];
                foreach($sequenceList as $subSequence){
                    $subOutputs[] = $this->run($subSequence, $output);
                }
                $output = $subOutputs;
            }else{
                // $sequenceList is a regular item
                $output = $sequenceList->run($output);
            }
        }

        return $output;
    }
}