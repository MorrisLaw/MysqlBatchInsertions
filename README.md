# MysqlBatchInsertions
Inserted data into mysql database by pooling 10,000 queries at a time and writing to the mysql database. This way was 1000 times faster at inserting data into the mysql database than, the python script, my team wrote before I came to this company. My program was able to read and write a 60gb json file into the MySql database in less than 6 hours. Whereas before it would've taken months, or even years. My team had this 60gb file for 2 years, since 2015, but couldn't figure out a way to write the 60gb of data in a reasonable amount of time. So I offered to do it. And did it successfully in one business day. 
