# Anomaly Detection: Flagged Purchases

## Task
Implement a social network where each user's purchases are kept track of. On demand, search the network for a persons's friends of degree `D` and aggregate the `T` most recent purchases. For more details, please see the full description in the [challenge repository](https://github.com/InsightDataScience/anomaly_detection).

## Approach
The social network is organized as an undirected graph, where the nodes are integer IDs and the edges are friend connections. The purchases are organized as a hash map of stacks, where the keys are the IDs and the stack units are tuples of integer indexes, timestamps and amounts.

When the stream of potentially anomalous purchases comes in, the persons' neighbors are found using a recursive implementation of breadth first search that keeps track of seen nodes as a class attribute, so that it can be used by all class methods. Recursion is used because the number of degrees corresponds to the levels of recursion, where BFS is `O(n)` for `n` nodes, and `d` is small enough to be about constant (since I figure I'm about 6 degrees from Kevin Bacon). To aggregate purchases of the neighbors, up to `T` items of the form `(time_index, time_stamp, amount)` are taken from each neighbor and added to an array, which is reverse sorted in-place in `O(nTlog(nT))` time, and the first `T` items are returned. The statistics of these are calculated and the passed entries go into a log file of flagged entries.

Two extra features were added. One is a log of invalid entries, which consists of data that did not pass a number of validation checks and appends it to a file. Currently it is just a dumping ground, but adding error messages would make it better. The other is adding a choice of how many standard deviations from the mean the anomalous price should be.

## User Guide
This script runs on Anaconda 4.X running on Python 3.5+. Anaconda comes with all of the modules used, but aside from the Python Standard Library, it only depends on Numpy v1.11.1 and NetworkX v1.11. A `requirements.txt` file has been included. Docstrings have been written for methods in the script, so feel free to take a look to get a more granular explanation of the thought process. The script runs from the project root by entering `./run.sh` into the terminal.

	usage: flagged_purchases.py [-h] --batch-file <filename> --stream-file
		                        <filename> --flag-file <filename> --invalid-file
		                        <filename> [--std-threshold <int>]

	This script builds a social network of friends and their purchases from batch
	data and then looks at stream data to flag purchases that are unusually high.

	optional arguments:
	  -h, --help            show this help message and exit
	  --batch-file <filename>
		                    filename for the input batch file. i.e.
		                    ../log_input/batch_log.json
	  --stream-file <filename>
		                    filename for the input stream file. i.e.
		                    ../log_input/stream_log.json
	  --flag-file <filename>
		                    filename for the output flagged file. i.e.
		                    ../log_output/flagged_purchases.json
	  --invalid-file <filename>
		                    filename for the invalid entries file. i.e.
		                    ../log_output/invalid_entries.txt
	  --std-threshold <int>
		                    The number of standard deviations to be above the mean
		                    for flagging. (default=3)

Unit tests have been written as part of test-driven development. They are in the `src/` folder along with `flagged_purchases.py`. It is added as part of `run.sh`.


