drop table if exists TRAFODION.odb_test.person1_dup;
CREATE TABLE TRAFODION.odb_test."PERSON1_DUP" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));INSERT INTO TRAFODION.odb_test."PERSON1_DUP" VALUES (1,'Fu-Jin','Pu','China','Xian',DATE'1968-10-29','U','apple@qq.edu',699649,'Google','bbbbbb',TIMESTAMP'2016-07-07 10:28:43');