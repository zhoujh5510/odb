drop table if exists TRAFODION.odb_test.sp_person11;
CREATE TABLE TRAFODION.odb_test."SP_PERSON11" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));