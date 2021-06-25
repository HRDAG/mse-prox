
The strata are all in the queue

# code here: 
- python to read next msg (long timeout), write sha.json to input, write sha.yaml to output
* Rscript gets input/sha.json, writes output/sha.json
* python gets sha, enqueues output/sha.json; deletes input/sha.json, output/sha.json, strata message 
