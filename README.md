# Power-Grep
String matching algorithm(KMP) for very large datasets using Mapreduce. For implementation details and analysis refer to [Presentation.pdf](https://github.com/h-sinha/Power-Grep/blob/main/Presentation.pdf).

### How to use
* Refer to [this link](https://www.digitalocean.com/community/tutorials/how-to-install-hadoop-in-stand-alone-mode-on-ubuntu-16-04) for Hadoop installation.
* Clone this repository
* Run following commands
```
cd Power-Grep/grep/src/main/java
<PATH TO HADOOP EXECUTABLE> com.sun.tools.javac.Main com/ds/Distributed.java
<PATH TO HADOOP EXECUTABLE> jar run.jar com.ds.Distributed <INPUT FILE CONTAINING STRING> <OUTPUT DIRECTORY> <SUBSTRING TO SEARCH>
cat out1/part*

```
OR simply edit [run.sh](https://github.com/h-sinha/Power-Grep/blob/main/grep/src/main/java/run.sh) and run it using ```bash run.sh```. By default [run.sh](https://github.com/h-sinha/Power-Grep/blob/main/grep/src/main/java/run.sh) is configured to search for the string "india" in a 96MB wikipedia dump.
