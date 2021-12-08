OUTPUT = EpSort
PROG_NAME = Sort
HDFS_FILE = Ep.txt

all:
	ant
	hadoop fs -rm -f $(HDFS_FILE)
	hadoop fs -put $(HDFS_FILE)
	hadoop fs -rm -r -f $(OUTPUT)
	hadoop jar output.jar $(PROG_NAME) $(HDFS_FILE) $(OUTPUT)

show:
	hadoop fs -cat $(OUTPUT)/part-r-00000

get:
	rm -rf tmp/$(OUTPUT)
	hadoop fs -get $(OUTPUT) tmp/$(OUTPUT)