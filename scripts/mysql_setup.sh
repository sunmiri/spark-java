#COnnect to MYSQL server using user/password
#mysql -u <user> -p <password> -h <host/localhost>

#On Sandbox MySql
mysql -uroot -phortonworks1 -hlocalhost
create database sparktest;
drop user sparktest;
CREATE USER 'sparktest'@'%' IDENTIFIED BY 'sparktest';
GRANT ALL PRIVILEGES ON * . * TO 'sparktest'@'%';
FLUSH PRIVILEGES;
exit;

#Swithing connected to new sparktest user
mysql -usparktest -psparktest -hlocalhost
use sparktest;

create table items (itemname char(50) not null, itemdesc char(50) not null, isactive tinyint(1) not null, createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

insert into items values ("Item1", 'ItemDesc1', 1, '2020-11-01 00:00:01');
insert into items values ("Item2", 'ItemDesc2', 1, '2020-11-02 00:00:01');
insert into items values ("Item3", 'ItemDesc3', 1, '2020-11-03 00:00:01');
insert into items values ("Item4", 'ItemDesc4', 1, '2020-11-01 00:00:01');

commit;

exit;