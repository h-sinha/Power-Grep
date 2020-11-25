unzip enwiki-latest-pages-articles26.zip
rm dis-positions.txt
rm seq-positions.txt
rm -rf out1
rm com/ds/*.class
/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main com/ds/Distributed.java
jar cf run.jar com/ds/*.class
time /usr/local/hadoop/bin/hadoop jar run.jar com.ds.Distributed enwiki-latest-pages-articles26.xml-p42567204p42663461 out1 india
echo "----------- Distributed code result -----------"
cat out1/part*
rm com/ds/*.class
/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main com/ds/Sequential.java
jar cf run.jar com/ds/*.class
echo "----------- Sequential code result -----------"
time /usr/local/hadoop/bin/hadoop jar run.jar com.ds.Sequential enwiki-latest-pages-articles26.xml-p42567204p42663461 out1 india