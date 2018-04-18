#Build the Jar
$mvn clean install
Jar will be generated in target/
# Run WordCount programme
hadoop jar /Jar/location/in/local/JarName.jar v3bigdata.beginner.wordcount.WordCount /inputfile/in/hdfs/filname.ext /outfile/in/hdfs/dirname
