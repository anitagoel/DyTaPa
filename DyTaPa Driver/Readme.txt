DyTaPa FW capabilities

-Can execute multiple tasks in parallel on a single data
-Each task can run process huge files
-task can run recursively while processing a small piece of the file, till it gets the full file processed
-the fw provide the recusive run capability, function developer need not worry for the same 
-data size for each iteration can be specified, for automatically taking care of the processing limitations of the cloud provider (like processing time (timeout) for each run)
-fw automtically determines number of iteratons basis the file size and data size it needs to prcoess in one iteration
-it ensures there is no dual billing for the fucntion controller and the function - only one instance stays active at a time 
-fw automatically invokes the next function instance after one function instance execution where there is further data remaining for prcoessing 
-it takes care of data file operations - reading data from a single input file
-a varity of operation can be run in parallel on a single data set, optput of all iteratons can be combined in a single result set
-separate output files generated for each operations at the specified location
-in this demonstration we have taken simple mathematical operations like calculating Maximum, Minimum and Addition on a huge input data file and generated separate output files while recursively processing the input file in separate parallel execution threads


Additonal capability in iteration 2
-capability to load developer function dynamically
-user doesnt have to worry about creating lambda fucntion or configuring the cloud environment
-user only has to focus on the simple function processing the data and generating the result
-user doesnt have to worry about file operations, recusive operations; every thing is handled by the fw itself
-user functions are stored in S3 in a serialized format and loaded and executed dynamically at runtime on the input data set
-provides capabilities to run any function on the data set without having to create the corresponding lambda function

