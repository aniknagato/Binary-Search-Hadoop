# Binary-Search-Hadoop
Binary search algorithm in distributed system using Map-Reduce algorithm in Hadoop for integers and float data.



Here is the procedure for running Distributed Binary Search:
1. Sample input file is given as input.txt
2. Change the extension of BinarySearch.txt to BinarySearch.java 
3. Make the java classes by running  “hadoop com.sun.tools.javac.Main BinarySearch.java”
4. Build the jar file by running “jar cf bs.jar BinarySearch*.class”
5. For running the jar file in hadoop by using “hadoop jar bs.jar BinarySearch /input_dir / /output_dir  target_value”
For example, the sample input file doesn’t contain 3. So, “hadoop jar bs.jar BinarySearch /input_dir / /output_dir 3” will write “3.0 0” in output file. And “-958357.371027” is in the sample input file. So “hadoop jar bs.jar BinarySearch /input_dir / /output_dir -958357.371027” will write “-958357.371027 1” in output file.
Java Version: 1.8.0_191
Hadoop Version: 2.7.7


