#!/usr/bin/env python
#-*- coding: UTF-8 -*- 

'''
File Name: 
     odb_auto_test.py
Description: 
     It test load, extract, copy and general function of odb untility.
     This script can create data file to load automantically in folder odb_auto_loadfile.
     Check result in odb_auto_log/odb_log_sumary_timestamp and details in odb_log_detail_timestamp
     create file	: scripts/ddl_person1_extract.sql ddl_person1.sql ddl_person2_extract.sql ddl_person2.sql ddl_person3_dup_test.sql ddl_person3_extract.sql ddl_person3.sql ddl_person_extract.sql ddl_person.sql script.sql
                      odb_auto_loadfile load_data_1 ~ load_data_12 load_data_bad
                      conf/export_tbl_list
     check result	: odb_auto_log/odb_log_sumary_timestamp odb_log_detail_timestamp
 Author: 
     liang.zhang@esgyn.cn
     jianhua.zhou@esgyn.cn
 FileCreatedTime: 
     2016.07.4
 LastModifiedTime: 
     2018.07.30
'''

import commands
import logging
import time
import os
import random
import sys, getopt
import signal


max_to_load = 1000
max_to_test_performance = 10000

preCmd = ""
#dbMysql=""
#usernameMysql=""
#passwdMysql=""

#dbOracle=""
#usernameOracle=""
#passwdOracle=""

dbTraf=""
usernameTraf=""
passwdTraf=""

#args , getopt
def opts_args_parse():
    opts, args = getopt.getopt(sys.argv[1:], "he:d:u:p:", ["preCmd=", "dbTraf=", "usernameTraf=", "passwdTraf="])
    print  len(opts)
    print args
    if len(opts) != 4:
        usage()
        sys.exit()
    for op, value in opts:
        if op == "-e":
            global preCmd
            preCmd = value
        elif op == "-d":
            global dbTraf
            dbTraf = value
        elif op == "-u":
            global usernameTraf
            usernameTraf = value
        elif op == "-p":
            global passwdTraf
            passwdTraf = value
        elif op == "-h":
            usage()
            sys.exit()
        elif op == "-help":
            usage()
            sys.exit()

def usage():
        print "usage short options example:"
        print "python odb_functional_test.py -i odb64luo -j traf -k trafodion -l traf123"
        print "e - -preCmd is location for odb64luo "
        print "d - -dbTraf is config ablout trafodion in odbc.ini"
        print "u - -usernameTraf is login username for trafodion"
        print "p - -passwdTraf is login passwd for trafodion"

opts_args_parse()


print
print "now prepare files/scripts/tables for odb test ..."
print 
# create logger
logger_sumary = logging.getLogger('sumary_log')
logger_sumary.setLevel(logging.DEBUG)
logger_detail = logging.getLogger('sumary_detail')
logger_detail.setLevel(logging.DEBUG)

#create log folder
folder = 'odb_auto_log'
if os.path.exists(folder):
    if os.path.isfile(folder):
        os.remove(folder)
        os.mkdir(folder)
else:
    os.mkdir(folder)

# create handler,to write log file
ISOTIMEFORMAT = '%Y-%m-%d-%H-%M-%S'
log_path_sum = folder + '/odb_log_sumary_' + str(time.strftime(ISOTIMEFORMAT))
sum_handle = logging.FileHandler(log_path_sum)
sum_handle.setLevel(logging.DEBUG)
log_path_detail = folder + '/odb_log_detail_' + str(time.strftime(ISOTIMEFORMAT))
detail_handle = logging.FileHandler(log_path_detail)
detail_handle.setLevel(logging.DEBUG)
print "now create log file"
print "sumary log file :" + log_path_sum
print "detail log file :" + log_path_detail
print 

# create handler,to write consle
consle_handle = logging.StreamHandler()
consle_handle.setLevel(logging.DEBUG)

# define handler format
# %(asctime)s - %(name)s - %(levelname)s - 
formatter = logging.Formatter('%(message)s')
sum_handle.setFormatter(formatter)
consle_handle.setFormatter(formatter)
detail_handle.setFormatter(formatter)

# add handler to logger
logger_sumary.addHandler(sum_handle)
logger_detail.addHandler(consle_handle)
logger_detail.addHandler(detail_handle)	

# create config file determine the list of src to be extracted
def create_config_file():
    config_folder = 'conf'
    if os.path.exists(config_folder):
        if os.path.isfile(config_folder):
            os.remove(config_folder)
            os.mkdir(config_folder)
    else:
        os.mkdir(config_folder)

    config_file = open("conf/export_tbl_list", "w")
    config_file.write("src=trafodion.odb_test_extract.person1_e\n")
    config_file.write("src=trafodion.odb_test_extract.person2_e\n")
    config_file.write("src=trafodion.odb_test_extract.person3_e\n")
    config_file.close()

    load_multi_file = file("conf/load_multi_list", "w")
    load_multi_file.write("src=multiload/multi_file1\n")
    load_multi_file.write("src=multiload/multi_file2\n")
    load_multi_file.write("src=multiload/multi_file3\n")
    load_multi_file.close()

def create_output():
    print "now create output folder"
    print "please wait..."

    output_folder = 'output_data'
    if os.path.exists(output_folder):
        if os.path.isfile(output_folder):
            os.remove(output_folder)
            os.mkdir(output_folder)
    else:
        os.mkdir(output_folder)

# create script for odb automantical test
def create_script():
    print "now create scripts for odb automantical test"
    print "please wait..."

    script_folder = 'scripts'
    if os.path.exists(script_folder):
        if os.path.isfile(script_folder):
            os.remove(script_folder)
            os.mkdir(script_folder)
    else:
        os.mkdir(script_folder)

    #script to test odb run a sql script
    script_file = open("scripts/script.sql", "w")
    script_file.write("SELECT COUNT(*) FROM trafodion.odb_test.person;")
    script_file.close()

    #script to create schema odb_test and table TRAFODION.odb_test.person
    script_file = open("scripts/ddl_person.sql", "w")
    script_file.write("create schema if not exists TRAFODION.ODB_TEST;\n")
    script_file.write("drop table if exists TRAFODION.odb_test.person;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."PERSON" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create schema odb_test and table TRAFODION.odb_test.person_1
    script_file = open("scripts/ddl_person_1.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.person_1;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."PERSON_1" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()
    
    #script to test view
    script_file = open("scripts/view.sql", "w")
    script_file.write("CREATE VIEW ODB_TEST_VIEW_1 AS SELECT * FROM ODB_TEST.PERSON;\n")
    script_file.write("CREATE VIEW ODB1_TEST_VIEW_2 AS SEELCT * FROM ODB_TEST.PERSON;")
    script_file.close()

    #script to create table TRAFODION.odb_test.person1
    script_file = open("scripts/ddl_person1.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.person1;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."PERSON1" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.person2
    script_file = file("scripts/ddl_person2.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.person2;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."PERSON2" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.person3
    script_file = file("scripts/ddl_person3.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.person3;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."PERSON3" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create schema odb_test_extract and table TRAFODION.odb_test_extract.person_e
    #this table is to test performance of different compression, should be large
    script_file = file("scripts/ddl_person_extract.sql", "w")
    script_file.write("create schema if not exists TRAFODION.ODB_TEST_EXTRACT;\n")
    script_file.write("drop table if exists TRAFODION.odb_test_extract.person_e;")
    script_file.write('CREATE TABLE TRAFODION.odb_test_extract."PERSON_E" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test_extract.person1_e
    script_file = file("scripts/ddl_person1_extract.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test_extract.person1_e;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test_extract."PERSON1_E" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test_extract.person2_e
    script_file = file("scripts/ddl_person2_extract.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test_extract.person2_e;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test_extract."PERSON2_E" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test_extract.person3_e
    script_file = file("scripts/ddl_person3_extract.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test_extract.person3_e;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test_extract."PERSON3_E" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.person3_dup
    script_file = file("scripts/ddl_person3_dup_test.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.person3_dup;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."PERSON3_DUP" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.write('INSERT INTO TRAFODION.odb_test."PERSON3_DUP" VALUES (1,\'Fu-Jin\',\'Pu\',\'China\',\'Xian\',DATE\'1968-10-29\',\'U\',\'apple@qq.edu\',699649,\'Google\',\'bbbbbb\',TIMESTAMP\'2016-07-07 10:28:43\');')
    script_file.close()

    #script to create table TRAFODION.odb_test.person1_dup
    script_file = file("scripts/ddl_person1_dup_test.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.person1_dup;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."PERSON1_DUP" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.write('INSERT INTO TRAFODION.odb_test."PERSON1_DUP" VALUES (1,\'Fu-Jin\',\'Pu\',\'China\',\'Xian\',DATE\'1968-10-29\',\'U\',\'apple@qq.edu\',699649,\'Google\',\'bbbbbb\',TIMESTAMP\'2016-07-07 10:28:43\');')
    script_file.close()

    #script to create table TRAFODION.odb_test.person2_dup
    script_file = file("scripts/ddl_person2_dup_test.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.person2_dup;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."PERSON2_DUP" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.write('INSERT INTO TRAFODION.odb_test."PERSON2_DUP" VALUES (1,\'Fu-Jin\',\'Pu\',\'China\',\'Xian\',DATE\'1968-10-29\',\'U\',\'apple@qq.edu\',699649,\'Google\',\'bbbbbb\',TIMESTAMP\'2016-07-07 10:28:43\');')
    script_file.close()

    script_file = file("scripts/ddl_sp_person1.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person1;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON1" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.ddl_sp_person2
    script_file = file("scripts/ddl_sp_person2.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person2;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON2" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()


    #script to create table TRAFODION.odb_test.ddl_sp_person3
    script_file = file("scripts/ddl_sp_person3.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person3;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON3" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.ddl_sp_person4
    script_file = file("scripts/ddl_sp_person4.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person4;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON4" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.ddl_sp_person5
    script_file = file("scripts/ddl_sp_person5.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person5;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON5" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.ddl_sp_person6
    script_file = file("scripts/ddl_sp_person6.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person6;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON6" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.ddl_sp_person7
    script_file = file("scripts/ddl_sp_person7.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person7;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON7" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.ddl_sp_person8
    script_file = file("scripts/ddl_sp_person8.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person8;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON8" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.ddl_sp_person9
    script_file = file("scripts/ddl_sp_person9.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person9;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON9" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.ddl_sp_person10
    script_file = file("scripts/ddl_sp_person10.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person10;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON10" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.ddl_sp_person11
    script_file = file("scripts/ddl_sp_person11.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person11;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON11" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.ddl_sp_person12
    script_file = file("scripts/ddl_sp_person12.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person12;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON12" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    # script to create table TRAFODION.odb_test.ddl_sp_person13
    script_file = file("scripts/ddl_sp_person13.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person13;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON13" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),ADDMORE CHAR(20),PRIMARY KEY (PID));')
    script_file.close()

    # script to create table TRAFODION.odb_test.ddl_sp_person14
    script_file = file("scripts/ddl_sp_person14.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person14;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON14" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    # script to create table TRAFODION.odb_test.ddl_sp_person15_1
    script_file = file("scripts/ddl_sp_person15_1.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person15_1;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON15_1" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    # script to create table TRAFODION.odb_test.ddl_sp_person15_2
    script_file = file("scripts/ddl_sp_person15_2.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person15_2;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON15_2" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    # script to create table TRAFODION.odb_test.ddl_sp_person15_3
    script_file = file("scripts/ddl_sp_person15_3.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person15_3;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON15_3" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    # script to create table TRAFODION.odb_test.ddl_sp_person15_4
    script_file = file("scripts/ddl_sp_person15_4.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_person15_4;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSON15_4" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to create table TRAFODION.odb_test.ddl_sp_personbad
    script_file = file("scripts/ddl_sp_personbad.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.sp_personbad;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."SP_PERSONBAD" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()

    #script to crate table TRAFODION.odb_test.load_multi_files
    script_file = file("scripts/ddl_load_multi_files.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.load_multi_files;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."LOAD_MULTI_FILES" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
    script_file.close()


    #script to crate table TRAFODION.odb_test.div_one, add by jianhua
    script_file = file("scripts/ddl_div_one.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.div_one;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."DIV_ONE"(C1 numeric(7,1));')
    script_file.close()

    #script to crate table TRAFODION.odb_test.div_more, add by jianhua
    script_file = file("scripts/ddl_div_more.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.div_more;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."DIV_MORE"(C1 numeric(7,1),C2 numeric(9,2));')
    script_file.close()

    #script to crate table TRAFODION.odb_test.fixed_one, add by jianhua
    script_file = file("scripts/ddl_fixed_one.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.fixed_one;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."FIXED_ONE"(C1 numeric(5,0));')
    script_file.close()

    #script to crate table TRAFODION.odb_test.fixed_more, add by jianhua
    script_file = file("scripts/ddl_fixed_more.sql", "w")
    script_file.write("drop table if exists TRAFODION.odb_test.fixed_more;\n")
    script_file.write('CREATE TABLE TRAFODION.odb_test."FIXED_MORE"(C1 numeric(5,0),C2 numeric(5,0),C3 numeric(4,0));')
    script_file.close()


    print("script create success")

#create a random bithday date by timestamp
def create_random_birthday(start, end):
    birthday_stamp = random.randint(start, end)
    timeArray = time.localtime(birthday_stamp)
    birthday = time.strftime("%Y-%m-%d", timeArray)
    return birthday

#this function is to create files to test load 
#file load_data_1 ~ load_data_12 to test load with differnt separator
#file load_data_bad to test load with parameter bad
def prepare_load_data(sp, fs, num):
    list_fname = ['Jian-Guo', 'Li-Ru', 'Shi-Fu', 'Bo', 'Liang', 'Yun-Peng', 'Fu-Jin']
    list_lname = ['Zhang', 'Yu', 'Zhou', 'Pu', 'Wu', 'Luo']
    list_country = ['China', 'US', 'Canada', 'Japan', 'India']
    list_city = ['Beijing', 'Xian', 'Shanghai', 'Chongqing', 'Shenzheng', 'Guangzhou', 'Guiyang']
    list_sex = ['M', 'F', 'U']
    list_employ = ['Google', 'Esgyn', 'Microsoft', 'HP', 'Apple', 'Tencent']
    list_note = ['aaaa', 'bbbbbb', 'cccccc']
    list_loadts = time.time()
    file_name = 0
    ret = 0

    timeArray = time.localtime(list_loadts)
    list_loadts_str = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

    #create folder of load data file
    input_folder = 'odb_auto_loadfile'
    if os.path.exists(input_folder):
        if os.path.isfile(input_folder):
            os.remove(input_folder)
            os.mkdir(input_folder)
    else:
        os.mkdir(input_folder)

    if (sp == '!'):
        file_name = '1'
    elif (sp == '@'):
        file_name = '2'
    elif (sp == '#'):
        file_name = '3'
    elif (sp == '$'):
        file_name = '4'
    elif (sp == '%'):
        file_name = '5'
    elif (sp == '^'):
        file_name = '6'
    elif (sp == '&'):
        file_name = '7'
    elif (sp == '*'):
        file_name = '8'
    elif (sp == ','):
        file_name = '9'
    elif (sp == '.'):
        file_name = '10'
    elif (sp == '"'):
        file_name = '11'
    elif (sp == '|'):
        file_name = '12'
    elif (sp == 'bad'):	# this file is to test load with parameter bad
        sp = ','
        file_name = 'bad'

    f = open(input_folder + '/load_data_' + file_name, 'w')
    for i in range(num):
        birthday = create_random_birthday(-2209017943, 1467648000) # create date between 1990-1-1 00:00:00 and 2016-7-5 00:00:00
        email = random.choice (['apple', 'pear', 'peach', 'orange', 'lemon']) + '@' + random.choice (['gmail', '163', 'sina', 'qq', 'esgyn']) + random.choice (['.com', '.cn', '.edu', '.org'])
        if (file_name == 'bad'):
            f.write(random.choice('123456789abcdefg') + sp + fs + random.choice(list_fname) + fs + sp + fs + random.choice(list_lname) + fs + sp + fs + random.choice(list_country) + fs + sp + fs + random.choice(list_city) + fs + sp + birthday + sp +  fs + random.choice(list_sex) + fs + sp + fs + email + fs + sp + str(random.randint(100000,2000000)) + sp + fs + random.choice(list_employ) + fs + sp + fs + random.choice(list_note) + fs + sp + list_loadts_str + '\n')
        else:
            f.write(str(i + 1) + sp + fs + random.choice(list_fname) + fs + sp + fs + random.choice(list_lname) + fs + sp + fs + random.choice(list_country) + fs + sp + fs + random.choice(list_city) + fs + sp + birthday + sp +  fs + random.choice(list_sex) + fs + sp + fs + email + fs + sp + str(random.randint(100000,2000000)) + sp + fs + random.choice(list_employ) + fs + sp + fs + random.choice(list_note) + fs + sp + list_loadts_str + '\n')
    f.close()
    return ret

def prepare_load_data_add(mutable,num):
    fs = ','
    list_fname = ['Jian-Guo', 'Li-Ru', 'Shi-Fu', 'Bo', 'Liang', 'Yun-Peng', 'Fu-Jin']
    list_lname = ['Zhang', 'Yu', 'Zhou', 'Pu', 'Wu', 'Luo']
    list_country = ['China', 'US', 'Canada', 'Japan', 'India']
    list_city = ['Beijing', 'Xian', 'Shanghai', 'Chongqing', 'Shenzheng', 'Guangzhou', 'Guiyang']
    list_sex = ['M', 'F', 'U']
    list_employ = ['Google', 'Esgyn', 'Microsoft', 'HP', 'Apple', 'Tencent']
    list_note = ['aaaa', 'bbbbbb', 'cccccc']
    list_loadts = time.time()
    file_name = 0
    ret = 0
    birthday = create_random_birthday(-2209017943, 1467648000) # create date between 1990-1-1 00:00:00 and 2016-7-5 00:00:00
    email = random.choice (['apple', 'pear', 'peach', 'orange', 'lemon']) + '@' + random.choice (['gmail', '163', 'sina', 'qq', 'esgyn']) + random.choice (['.com', '.cn', '.edu', '.org'])

    timeArray = time.localtime(list_loadts)
    list_loadts_str = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    input_folder = 'odb_auto_loadfile'
    if (mutable=='nullstring'):
        file_name = '13'
        f = open(input_folder + '/load_data_' + file_name,'w')
        for i in range(num):
            f.write(str(i + 1) + fs + random.choice(list_fname) + fs + random.choice(list_lname) + fs + random.choice(list_country) + fs + random.choice(list_city) + fs + birthday + fs + random.choice(list_sex) + fs + email + fs + str(random.randint(100000, 2000000)) + fs + random.choice(list_employ) + fs + random.choice(list_note) + fs +  list_loadts_str + fs + mutable + '\n')
        f.close()
    elif (mutable=='delay'):
        file_name='14'
        f = open(input_folder + '/load_data_' + file_name, 'w')
        for i in range(num):
            f.write(str(i + 1) + fs + random.choice(list_fname) + fs + random.choice(list_lname) + fs + random.choice(list_country) + fs + random.choice(list_city) + fs + birthday + fs + random.choice(list_sex) + fs + email + fs + str(random.randint(100000, 2000000)) + fs + random.choice(list_employ) + fs + random.choice(list_note) + fs + list_loadts_str + fs + '\n')
        f.close()



def prepare_multi_files_data():
    sp='|'
    fs='"'
    list_fname = ['Jian-Guo', 'Li-Ru', 'Shi-Fu', 'Bo', 'Liang', 'Yun-Peng', 'Fu-Jin']
    list_lname = ['Zhang', 'Yu', 'Zhou', 'Pu', 'Wu', 'Luo']
    list_country = ['China', 'US', 'Canada', 'Japan', 'India']
    list_city = ['Beijing', 'Xian', 'Shanghai', 'Chongqing', 'Shenzheng', 'Guangzhou', 'Guiyang']
    list_sex = ['M', 'F', 'U']
    list_employ = ['Google', 'Esgyn', 'Microsoft', 'HP', 'Apple', 'Tencent']
    list_note = ['aaaa', 'bbbbbb', 'cccccc']
    list_loadts = time.time()
    file_name = 0
    ret = 0

    timeArray = time.localtime(list_loadts)
    list_loadts_str = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

    #create folder of load data file
    multi_folder = 'multiload'
    if os.path.exists(multi_folder):
        if os.path.isfile(multi_folder):
            os.remove(multi_folder)
            os.mkdir(multi_folder)
    else:
        os.mkdir(multi_folder)

    f = open(multi_folder + '/multi_file1', 'w')
    for i in range(1, 100):
        birthday = create_random_birthday(-2209017943, 1467648000) # create date between 1990-1-1 00:00:00 and 2016-7-5 00:00:00
        email = random.choice (['apple', 'pear', 'peach', 'orange', 'lemon']) + '@' + random.choice (['gmail', '163', 'sina', 'qq', 'esgyn']) + random.choice (['.com', '.cn', '.edu', '.org'])
        f.write(str(i + 1) + sp + fs + random.choice(list_fname) + fs + sp + fs + random.choice(list_lname) + fs + sp + fs + random.choice(list_country) + fs + sp + fs + random.choice(list_city) + fs + sp + birthday + sp +  fs + random.choice(list_sex) + fs + sp + fs + email + fs + sp + str(random.randint(100000,2000000)) + sp + fs + random.choice(list_employ) + fs + sp + fs + random.choice(list_note) + fs + sp + list_loadts_str + '\n')
    f.close()

    f = open(multi_folder + '/multi_file2', 'w')
    for i in range(101, 200):
        birthday = create_random_birthday(-2209017943, 1467648000) # create date between 1990-1-1 00:00:00 and 2016-7-5 00:00:00
        email = random.choice (['apple', 'pear', 'peach', 'orange', 'lemon']) + '@' + random.choice (['gmail', '163', 'sina', 'qq', 'esgyn']) + random.choice (['.com', '.cn', '.edu', '.org'])
        f.write(str(i + 1) + sp + fs + random.choice(list_fname) + fs + sp + fs + random.choice(list_lname) + fs + sp + fs + random.choice(list_country) + fs + sp + fs + random.choice(list_city) + fs + sp + birthday + sp +  fs + random.choice(list_sex) + fs + sp + fs + email + fs + sp + str(random.randint(100000,2000000)) + sp + fs + random.choice(list_employ) + fs + sp + fs + random.choice(list_note) + fs + sp + list_loadts_str + '\n')
    f.close()

    f = open(multi_folder + '/multi_file3', 'w')
    for i in range(201, 300):
        birthday = create_random_birthday(-2209017943, 1467648000) # create date between 1990-1-1 00:00:00 and 2016-7-5 00:00:00
        email = random.choice (['apple', 'pear', 'peach', 'orange', 'lemon']) + '@' + random.choice (['gmail', '163', 'sina', 'qq', 'esgyn']) + random.choice (['.com', '.cn', '.edu', '.org'])
        f.write(str(i + 1) + sp + fs + random.choice(list_fname) + fs + sp + fs + random.choice(list_lname) + fs + sp + fs + random.choice(list_country) + fs + sp + fs + random.choice(list_city) + fs + sp + birthday + sp +  fs + random.choice(list_sex) + fs + sp + fs + email + fs + sp + str(random.randint(100000,2000000)) + sp + fs + random.choice(list_employ) + fs + sp + fs + random.choice(list_note) + fs + sp + list_loadts_str + '\n')
    f.close()

    return ret


#create tables to test extract 
#schema : odb_test_extract
#tables : person_e, person1_e, person2_e, person3_e
def prepare_extract_data():
    print "now create tables to test extract functions"
    print "please wait..."
    global preCmd
    global dbTraf
    global usernameTraf
    global passwdTraf
    print preCmd
    print dbTraf
    print usernameTraf
    print passwdTraf
    cmd = preCmd + ' -u ' +  usernameTraf + ' -p ' + passwdTraf + ' -d ' + dbTraf +  ' -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person_extract.sql:tgt=trafodion.odb_test_extract.person_e:max=1000000:rows=5000:loadcmd=UL:fs=,:sq=\\" -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person1_extract.sql:tgt=trafodion.odb_test_extract.person1_e:max=1000:rows=5000:loadcmd=UL:fs=,:sq=\\" -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person2_extract.sql:tgt=trafodion.odb_test_extract.person2_e:max=1000:rows=5000:loadcmd=UL:fs=,:sq=\\" -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3_extract.sql:tgt=trafodion.odb_test_extract.person3_e:max=1000:rows=5000:loadcmd=UL:fs=,:sq=\\"'
    #cmd = 'odb64luo -u trafodion -p traf123 -d traf  -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person_extract.sql:tgt=trafodion.odb_test_extract.person_e:max=1000000:rows=5000:parallel=1:loadcmd=UL:fs=,:sq=\\" -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person1_extract.sql:tgt=trafodion.odb_test_extract.person1_e:max=1000:rows=5000:parallel=1:loadcmd=UL:fs=,:sq=\\" -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person2_extract.sql:tgt=trafodion.odb_test_extract.person2_e:max=1000:rows=5000:parallel=1:loadcmd=UL:fs=,:sq=\\" -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3_extract.sql:tgt=trafodion.odb_test_extract.person3_e:max=1000:rows=5000:parallel=1:loadcmd=UL:fs=,:sq=\\"'
    (status, output) = commands.getstatusoutput(cmd)
    if status == 0:
        print "table to extract create success"
        return status
    else:
        print "table to extract create failed"
        print output
        return status

def execute_cmd(num, usecase, cmd, expectedvalue=''):
    session_start = 0
    session_end = 0
    session_start = time.time()
    global output
    (status, output) = commands.getstatusoutput(cmd)
    session_end = time.time()
    logger_sumary.info('------------------ test : %d ------------------', num)
    logger_detail.info('------------------ test : %d ------------------', num)
    logger_sumary.info('Usecase : %s', usecase)
    logger_detail.info('Usecase : %s', usecase)
    logger_sumary.info('Command : %s', cmd)
    logger_detail.info('Command : %s', cmd)

    if status == 0:
        if output.find('Err') != -1:
            status = -1
            logger_sumary.info('Result : failed')
            logger_detail.info('Result : failed')
        elif expectedvalue != '':
            if output.find(expectedvalue) != -1:
                logger_sumary.info('Result : success')
                logger_detail.info('Result : success')
            else:
                status = -1
                logger_sumary.info('Result : failed')
                logger_detail.info('Result : failed')
        else:
            logger_sumary.info('Result : success')
            logger_detail.info('Result : success')
    else:
        logger_sumary.info('Result : failed')
        logger_detail.info('Result : failed')

    logger_sumary.info('During : %ds\n', session_end - session_start)
    logger_detail.info('During : %ds', session_end - session_start)

    logger_detail.info('Details :\n%s\n', output)
    print
    return status

def set_timeout(num, callback):
    def wrap(func):
        def handle(signum, frame):
                raise RuntimeError

        def to_do(*args, **kwargs):
            try:
                signal.signal(signal.SIGTERM, handle)
                signal.alarm(num)
                print 'start alarm signal.'
                r = func(*args, **kwargs)
                print 'close alarm signal.'
                signal.alarm(0)
                return r
            except Exception as result:
                callback()
		print result

        return to_do

    return wrap

def after_timeout():
    print 'Leave interactive mode through timeout'

@set_timeout(60, after_timeout)
def execute_cmd_timeout(num, usecase, cmd, expectedvalue=''):
    session_start = 0
    session_end = 0
    session_start = time.time()
    global output
    (status, output) = commands.getstatusoutput(cmd)
    session_end = time.time()
    logger_sumary.info('------------------ test : %d ------------------', num)
    logger_detail.info('------------------ test : %d ------------------', num)
    logger_sumary.info('Usecase : %s', usecase)
    logger_detail.info('Usecase : %s', usecase)
    logger_sumary.info('Command : %s', cmd)
    logger_detail.info('Command : %s', cmd)

    if status == 0:
        if output.find('Error') != -1:
            status = -1
            logger_sumary.info('Result : failed')
            logger_detail.info('Result : failed')
        elif expectedvalue != '':
            if output.find(expectedvalue) != -1:
                logger_sumary.info('Result : success')
                logger_detail.info('Result : success')
            else:
                status = -1
                logger_sumary.info('Result : failed')
                logger_detail.info('Result : failed')
        else:
            logger_sumary.info('Result : success')
            logger_detail.info('Result : success')
    else:
        logger_sumary.info('Result : failed')
        logger_detail.info('Result : failed')

    logger_sumary.info('During : %ds\n', session_end - session_start)
    logger_detail.info('During : %ds', session_end - session_start)

    logger_detail.info('Details :\n%s\n', output)
    print
    return status

#add by zhoujianhua
def execute_cmd_other(num, usecase, cmd, expectedvalue=''):
    session_start = 0
    session_end = 0
    session_start = time.time()
    global output
    (status, output) = commands.getstatusoutput(cmd)
    session_end = time.time()
    logger_sumary.info('------------------ test : %d ------------------', num)
    logger_detail.info('------------------ test : %d ------------------', num)
    logger_sumary.info('Usecase : %s', usecase)
    logger_detail.info('Usecase : %s', usecase)
    logger_sumary.info('Command : %s', cmd)
    logger_detail.info('Command : %s', cmd)

    if status == 0:
        if output.find('Error') != -1:
            status = -1
            logger_sumary.info('Result : failed')
            logger_detail.info('Result : failed')
        elif expectedvalue != '':
            if output.find(expectedvalue) != -1:
                logger_sumary.info('Result : success')
                logger_detail.info('Result : success')
            else:
                status = -1
                logger_sumary.info('Result : failed')
                logger_detail.info('Result : failed')
        else:
            logger_sumary.info('Result : success')
            logger_detail.info('Result : success')
    elif status > 0:
        if output.find(expectedvalue) != -1:
            status = 0
            logger_sumary.info('Result : success')
            logger_detail.info('Result : success')
        else:
            status = -1
            logger_sumary.info('Result : failed')
            logger_detail.info('Result : failed')
    else:
        logger_sumary.info('Result : failed')
        logger_detail.info('Result : failed')

    logger_sumary.info('During : %ds\n', session_end - session_start)
    logger_detail.info('During : %ds', session_end - session_start)

    logger_detail.info('Details :\n%s\n', output)
    print
    return status



def main():
    global max_to_load
    global max_to_test_performance

    total_num = 0
    failed_num = 0
    success_num = 0
    ret = 0
    start_time = 0
    end_time = 0

    start_time = time.time()

    #preCmd="odb64luo "
    #database=" traf "
    #username=" trafodion "
    #passwd=" traf123 "
    global preCmd
    global dbTraf
    global usernameTraf
    global passwdTraf
    database=dbTraf
    username=usernameTraf
    passwd=passwdTraf

    #test 1
    total_num += 1
    cmd = preCmd + ' -u ' + username +  ' -p ' + passwd  + ' -d ' + database  +' -i'
    usecase = 'Connect trafodion as the data source'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
    '''    
    #test 2
    total_num += 1
    cmd = preCmd + ' -u ' + username +  ' -p ' + passwd  + ' -d ' + database  +' -I'
    usecase = 'Connect trafodion with sql interpreter mode'
    def after_timeout():
        print 'Leave interactive mode through timeout !'
    ret = execute_cmd_timeout(total_num, usecase, cmd, 'SQL Interpreter Mode')
    if ret != 0:
        failed_num += 1
    '''

    #test 2 ,changed by jianhua, execute_cmd_other if success, the value of status is 0, else -1
    total_num += 1
    cmd = preCmd + ' -u ' + username + 'lijian -p ' + passwd + ' -d ' + database + ' -i'
    usecase = 'Connect trafodion with error username'
    ret = execute_cmd_other(total_num, usecase, cmd, 'SQL ERROR')
    if ret == -1:
        failed_num +=1
    else:
        success_num +=1
    
    #test 3 , changed by jianhua
    total_num += 1
    cmd = preCmd + ' -p ' + passwd + ' -d ' + database + ' -i'
    usecase = 'Connect trafodion miss username'
    ret = execute_cmd_other(total_num, usecase, cmd, 'Missing User')
    if ret == -1 or output.find('Missing User') == -1:
        failed_num +=1
    else:
        success_num +=1
    
      
    #test 4 , changed by jianhua, the returned value of this case is not 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + 'lijian -d ' + database + ' -i'
    usecase = 'Connect trafodion with error passwd'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 or output.find('Invalid username or password') == -1:
        failed_num +=1
    else:
        success_num +=1


    '''    
    #test 6
    #suggest this case is runing by handmade
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -d ' + database + ' -i'
    usecase = 'Connect trafodion miss passwd'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret == -1:
        failed_num +=1
    '''    


    #test 5 , changed by jianhua
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + 'lijian -i'
    usecase = 'Connect trafodion with error DSN'
    ret = execute_cmd_other(total_num, usecase, cmd, 'Data source name not found')
    if ret != 0:
        failed_num +=1
    else:
        success_num +=1

    #test 6 , changed by jianhua
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -i'
    usecase = 'Connect trafodion miss DSN'
    ret = execute_cmd_other(total_num, usecase, cmd, 'Missing DSN')
    if ret != 0:
        failed_num +=1
    else:
        success_num +=1
        
    #test 7, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p '+ passwd + ' -ca ' + '"DSN=' + database +'" -i'
    usecase = 'Connect trafodion by ca'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num +=1
    else:
        success_num +=1
    
    #test 8, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -version'
    usecase = 'run odb -version'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num +=1
    else:
        success_num +=1
    
    #test 9, the value of status returned 0
    total_num += 1
    #cmd = preCmd + '-u trafodion -p traf123 -d traf -i c'
    cmd = preCmd + ' -u '+ username + ' -p ' +  passwd +  ' -d ' +  database + ' -i c'
    usecase = 'List catalogs'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 or output.find('TRAFODION') == -1:
        failed_num += 1
    else:
        success_num +=1

    #test 10, the value of status returned 0
    total_num += 1
    #cmd = preCmd + '-u trafodion -p traf123 -d traf -i s:trafodion'
    cmd = preCmd + ' -u '+ username + ' -p ' + passwd + ' -d ' + database + ' -i s:trafodion'
    usecase = 'List schemas in specific catalg'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 or output.find('SEABASE') == -1:
        failed_num += 1
    else:
        success_num +=1
        
    #test 11, the value of status returned 0
        #first require to create person.map file and also create input_data dicrectory with datasets
    total_num += 1
    #cmd = preCmd + '-l src=nofile:pre=@scripts/ddl_person.sql:tgt=trafodion.odb_test.person:max=10000:map=person.map:rows=5000:parallel=1:loadcmd=UL -u trafodion -p traf123 -d traf'
    cmd = preCmd + ' -l src=nofile:pre=@scripts/ddl_person.sql:tgt=trafodion.odb_test.person:max=10000:map=person.map:rows=5000:parallel=1:loadcmd=UL -u ' +  username + ' -p ' +  passwd + ' -d ' +  database
    usecase = 'Generating and Loading Data with map but no data file'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 and output.find('Error') != -1:
        failed_num += 1
    else:
        success_num +=1

    #test 12, changed by jianhua
    total_num += 1
    #cmd = preCmd + '-l src=nofile:pre=@scripts/ddl_person_1.sql:tgt=trafodion.odb_test.person_1:max=10000:map=person.map:rows=5000:parallel=1:loadcmd=UL -u trafodion -p traf123 -d traf'
    cmd = preCmd + ' -timeout 1 -l src=nofile:pre=@scripts/ddl_person_1.sql:tgt=trafodion.odb_test.person_1:max=1000000:map=person.map:rows=5000:parallel=1:loadcmd=UL -u ' +  username + ' -p ' +  passwd + ' -d ' +  database
    usecase = 'Generating and Loading Data with map but no data file with timeout'
    ret = execute_cmd_other(total_num, usecase, cmd, 'Received SIGALRM (timeout) after 1s')
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
        
    #test 13, the value of status returned 0
    total_num += 1
    #cmd = preCmd + '-u trafodion -p traf123 -d traf -i t:trafodion.odb_test'
    cmd = preCmd + ' -u '+ username + ' -p ' + passwd + ' -d ' + database + ' -i t:trafodion.odb_test'
    usecase = 'List tables in specific schema'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 14, the value of status returned 0
    total_num += 1
    #cmd = preCmd + '-u trafodion -p traf123 -d traf -i v:trafodion.odb_test.%1'
    cmd = preCmd + ' -u '+ username + ' -p ' + passwd + ' -d ' + database + ' -i v:trafodion.odb_test.%1'
    usecase = 'List views in specific scehma and viewname end with 1'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
            
    #test 15, the value of status returned 0
    total_num += 1
    #cmd = preCmd + '-u trafodion -p traf123 -d traf -i T:trafodion.odb_test.person%'
    cmd = preCmd + ' -u '+ username + ' -p ' + passwd + ' -d ' + database + ' -i T:trafodion.odb_test.person%'
    usecase = 'List tables in specific scehma and tablename start with person'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
        
    #test 16, the value of status returned 0
    total_num += 1
    #cmd = preCmd + '-u trafodion -p traf123 -d traf -i D:trafodion.odb_test.person'
    cmd = preCmd + ' -u '+ username + ' -p ' + passwd + ' -d ' + database + ' -i D:trafodion.odb_test.person'
    usecase = 'show disctrible about specific table'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 17, the value of status returned 0
    total_num += 1
    #cmd = preCmd + '-u trafodion -p traf123 -d traf -i U2,4:trafodion.odb_test.person'
    cmd = preCmd + ' -u '+ username + ' -p ' + passwd + ' -d ' + database + ' -i U2,4:trafodion.odb_test.person'
    usecase = 'multiplies the length of "non-wide" text fields by 2 and the length of wide text fields by 4.'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
            
            
    #test 18, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -lsdrv;'+preCmd+' -lsdsn'
    usecase = 'List Available ODBC Drivers and Data Sources'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

        
    #test 19, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -f scripts/script.sql ' + ' -u ' + username + ' -p ' +  passwd + ' -d ' +  database + ' -q cmd'
    usecase = 'test -q cmd'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
        
    #test 20, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -f scripts/script.sql ' + ' -u ' + username + ' -p ' +  passwd + ' -d ' +  database + ' -q cmd'
    usecase = 'test -q cmd'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
    
    #test 21, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -f scripts/script.sql ' + ' -u ' + username + ' -p ' +  passwd + ' -d ' +  database + ' -q all'
    usecase = 'test -q cmd'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 22,  the value of status returned 0
        #first require to create person1.map,person2.map,person3.map file and also create input_data dicrectory with datasets
    total_num += 1
    #cmd = preCmd + '-u trafodion -p traf123 -d traf  -l src=nofile:pre=@scripts/ddl_person1.sql:tgt=trafodion.odb_test.person1:max=10000:map=person1.map:rows=5000:parallel=1:loadcmd=UL -l src=nofile:pre=@scripts/ddl_person2.sql:tgt=trafodion.odb_test.person2:max=10000:map=person2.map:rows=5000:parallel=1:loadcmd=UL -l src=nofile:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=10000:map=person3.map:rows=5000:parallel=1:loadcmd=UL'
    cmd = preCmd + ' -u ' + username + ' -p ' +  passwd + ' -d ' + database + ' -l src=nofile:pre=@scripts/ddl_person1.sql:tgt=trafodion.odb_test.person1:max=10000:map=person1.map:rows=5000:parallel=1:loadcmd=UL -l src=nofile:pre=@scripts/ddl_person2.sql:tgt=trafodion.odb_test.person2:max=10000:map=person2.map:rows=5000:parallel=1:loadcmd=UL -l src=nofile:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=10000:map=person3.map:rows=5000:parallel=1:loadcmd=UL'
    usecase = 'Generating and Loading multiple Data tables with map but no data file'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 or output.find('Err') != -1:
        failed_num += 1
    else:
        success_num +=1

    #test 23, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_1:pre=@scripts/ddl_sp_person1.sql:tgt=trafodion.odb_test.sp_person1:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=\\!:sq=\\"'
    usecase = 'load with ! as sp'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 or output.find('Err') != -1:
        failed_num += 1
    else:
        success_num +=1

    #test 24,  the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_2:pre=@scripts/ddl_sp_person2.sql:tgt=trafodion.odb_test.sp_person2:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=@:sq=\\"'
    usecase = 'load with @ as sp'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 or output.find('Err') != -1:
        failed_num += 1
    else:
        success_num +=1

    #test 25, changed by jianhua, this case should be juged again
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_3:pre=@scripts/ddl_sp_person3.sql:tgt=trafodion.odb_test.sp_person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=#:sq=\\"'
    usecase = 'load with # as sp, expected result is failed!!!!'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 or output.find('Err') != -1:
        failed_num += 1
    else:
        success_num +=1

    #test 26, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_4:pre=@scripts/ddl_sp_person4.sql:tgt=trafodion.odb_test.sp_person4:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=$:sq=\\"'
    usecase = 'load with $ as sp'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 27, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_5:pre=@scripts/ddl_sp_person5.sql:tgt=trafodion.odb_test.sp_person5:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=%:sq=\\"'
    usecase = 'load with % as sp'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 28, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_6:pre=@scripts/ddl_sp_person6.sql:tgt=trafodion.odb_test.sp_person6:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=^:sq=\\"'
    usecase = 'load with ^ as sp'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 29, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_7:pre=@scripts/ddl_sp_person7.sql:tgt=trafodion.odb_test.sp_person7:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=\\&:sq=\\"'
    usecase = 'load with & as sp'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 30, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_8:pre=@scripts/ddl_sp_person8.sql:tgt=trafodion.odb_test.sp_person8:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=*:sq=\\"'
    usecase = 'load with * as sp'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 31, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_sp_person9.sql:tgt=trafodion.odb_test.sp_person9:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=,:sq=\\"'
    usecase = 'load with , as sp'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 32, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_10:pre=@scripts/ddl_sp_person10.sql:tgt=trafodion.odb_test.sp_person10:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=.:sq=\\"'
    usecase = 'load with . as sp'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 33, changed by jianhua
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_11:pre=@scripts/ddl_sp_person11.sql:tgt=trafodion.odb_test.sp_person11:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=\\":sq=\\"'
    usecase = 'load with " as sp, the expected of this case is failed!!!'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 or output.find('Err') == -1:
        failed_num += 1
    else:
        success_num +=1

    #test 34, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_12:pre=@scripts/ddl_sp_person12.sql:tgt=trafodion.odb_test.sp_person12:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=\\|:sq=\\"'
    usecase = 'load with | as sp'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    # test 35, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_13:pre=@scripts/ddl_sp_person13.sql:tgt=trafodion.odb_test.sp_person13:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=\\,:ns=nullstring'
    usecase = 'load with ns'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    # test 36, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -delay 1000 -l src=odb_auto_loadfile/load_data_13:pre=@scripts/ddl_sp_person14.sql:tgt=trafodion.odb_test.sp_person14:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=\\,:parallel=3'
    usecase = 'load with -delay'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    # test 37, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -f scripts/ddl_sp_person15_1.sql -f scripts/ddl_sp_person15_2.sql -f scripts/ddl_sp_person15_3.sql -f scripts/ddl_sp_person15_4.sql -T 2 -dlb'
    usecase = 'load with -dlb'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 or output.count("1.0.0") != 2:
        failed_num += 1
    else:
        success_num +=1

    # test 38, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -P scripts/ddl_sp_person15* -T 2'
    usecase = 'create table with -P'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    # test 39, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -S scripts/ddl_sp_person15* -c'
    usecase = 'create table with -S'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    # test 40, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -S scripts/ddl_sp_person15* -c -Z'
    usecase = '-Z: in random order'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1



    #test 41, changed by jianhua, this test case should be juged again
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_bad:pre=@scripts/ddl_sp_personbad.sql:tgt=trafodion.odb_test.sp_personbad:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=IN:fs=,:sq=\\":bad=output_data/bad_records -v'
    usecase = 'Load bad records and output specified bad records output file, the expected of this case is failed!!'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 or output.find('Err') != -1:
        failed_num += 1
    else:
        success_num +=1

    #test 42, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person1_dup_test.sql:tgt=trafodion.odb_test.person1_dup:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=,:sq=\\" -v'
    usecase = 'Load duplicate data using upsert can successfully'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 43, changed by jianhua, this test case should be juged again
        #my practice show success
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person2_dup_test.sql:tgt=trafodion.odb_test.person2_dup:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=IN:fs=,:sq=\\" -v'
    usecase = 'Load duplicate data using insert will fail, this expected of this case is failed!!!'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 or output.find('Err') != -1:
        failed_num += 1
    else:
        success_num +=1

    #test 44, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_nor_%t.csv:rows=m10:fs=,:trim:sq=\\"'
    usecase = 'Extract single table using default format'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 45, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_xml_%t.csv:rows=m10:fs=,:trim:sq=\\":xml'
    usecase = 'Extract single table using xml format'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 46, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_gzip_%t.csv.gz:rows=m10:fs=,:trim:sq=\\":gzip'
    usecase = 'Extract single table using gzip format'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 47, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -e src=-conf/export_tbl_list:tgt=output_data/ext_multi_%t.csv:rows=m10:fs=,:trim:sq=\\"'
    usecase = 'Extract multiple data tables using default format'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 48, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -e src=-conf/export_tbl_list:tgt=output_data/ext_multi_sub_%t.csv:rows=m10:fs=,:trim:sq=\\":pwhere=[country=\\\'Canada\\\'] -v'
    usecase = 'Extract interested subset of rows by pwhere'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 49, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_wb9_gzip_%t.csv.gz:rows=m10:fs=,:trim:sq=\\":gzip:gzpar=wb9'
    usecase = 'Extract with compression algorithm wb9'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 50, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_wb1_gzip_%t.csv.gz:rows=m10:fs=,:trim:sq=\\":gzip:gzpar=wb1'
    usecase = 'Extract with compression algorithm wb1'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 51, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_wb6h_gzip_%t.csv.gz:rows=m10:fs=,:trim:sq=\\":gzip:gzpar=wb6h'
    usecase = 'Extract with compression algorithm wb6h'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 52, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_wb6R_gzip_%t.csv.gz:rows=m10:fs=,:trim:sq=\\":gzip:gzpar=wb6R'
    usecase = 'Extract with compression algorithm wb6R'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
        
    #test 53, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select count(*) from trafodion.odb_test.person"'
    usecase = 'Run a sql command'
    ret = execute_cmd(total_num, usecase, cmd, '1 row(s)')
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 54, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select count(*) from trafodion.odb_test.person" -vv'
    usecase = '-vv'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0 or output.find('etab[0]') == -1:
        failed_num += 1
    else:
        success_num +=1

    # test 55, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select * from trafodion.odb_test.person limit 2" -pcn'
    usecase = 'result with column name'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 or output.find('PID') == -1:
        failed_num += 1
    else:
        success_num +=1

    # test 56, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select * from trafodion.odb_test.person limit 2" -pad'
    usecase = 'Generates the output in table format'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0 or output.find('|') == -1:
        failed_num += 1
    else:
        success_num +=1

    #test 57, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select count(*) from trafodion.odb_test.person" -c'
    usecase = 'Run a sql command and get csv output '
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 58, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select count(*) from trafodion.odb_test.person" -c -b'
    usecase = '-b : print start time in the headers when CSV output'
    ret = execute_cmd(total_num, usecase, cmd, 'Thread id [')
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
        
    #test 59, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select * from trafodion.odb_test.person limit 100" -r 10'
    usecase = 'Run select with rowset '
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 60, chnaged by jianhua, this test case should juged again
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select * from trafodion.odb_test.person" -plm'
    usecase = '-plm Print Line Mode '
    ret = execute_cmd_other(total_num, usecase, cmd, 'ROW 1')
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    # test 61, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select * from trafodion.odb_test.person" -F 2'
    usecase = ' -F max rows to fetch'
    ret = execute_cmd(total_num, usecase, cmd, '2 row(s)')
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    # test 62, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x 5:"select * from trafodion.odb_test.person" -ttime 10000 -T 1'
    usecase = '-ttime delay(ms) before starting next command'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    # test 63, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x 5:"select * from trafodion.odb_test.person" -ttime 5000:10000 -T 1'
    usecase = '-ttime delay(ms) before starting next command in a thread random delay if a [min:max] range is specified'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    # test 64, chnaged by jianhua, this test case should be juged again
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select * from trafodion.odb_test.person" -noconnect'
    usecase = '-noconnect do not connect on startup General options'
    ret = execute_cmd_other(total_num, usecase, cmd, 'Connection required')
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    # test 65, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select * from trafodion.odb_test.person" -L 2'
    usecase = '-L run everything #loops times'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
        
    #test 66, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select * from trafodion.odb_test.sp_person2 limit 100" -fs @'
    usecase = 'Run select with -fs @ '
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
        
    #test 67, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select * from trafodion.odb_test.sp_person2 limit 100" -fs 0100'
    usecase = 'Run select with -fs 0100 '
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
        
    #test 68, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select * from trafodion.odb_test.sp_person2 limit 100" -fs X40'
    usecase = 'Run select with -fs X40 '
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
        
    #test 69, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select * from trafodion.odb_test.sp_person2 limit 100" -rs X40'
    usecase = 'Run select with -fs X40 '
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1
        
    #test 70, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -N -x "delete from trafodion.odb_test.person"'
    usecase = 'Prepare a sql command '
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0 or output.find('prepared') == -1:
        failed_num += 1
    else:
        success_num +=1

    #test 71, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -f scripts/script.sql'
    usecase = 'Run a sql script'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 72, the value of status returned 0
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -x "select count(*) from trafodion.odb_test.person1" -f scripts/script.sql'
    usecase = 'Parallelize multiple commands and scripts'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 73, changed by jianhua, this test case should be juged again
    total_num += 1
    #cmd = 'odb64luo -u trafuser -p trafpasswd -d trafdb  -l src=-odb_load_file:tgt=trafodion.odb_test.sp_person3:max=10000:rows=5000:parallel=1:loadcmd=UL:fs=\|:sq=\"'
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=-conf/load_multi_list:pre=@scripts/ddl_load_multi_files.sql:tgt=trafodion.odb_test.load_multi_files:max=10000:rows=5000:parallel=1:loadcmd=UL:fs=\\|:sq=\\"'
    usecase = 'load multi files to trafodion table'
    ret = execute_cmd(total_num, usecase, cmd)
    if ret != 0 or output.find('Err') != -1:
        failed_num += 1
    else:
        success_num +=1



    #test 74, write by jianhua, the next cases are about functional of div
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/div_one:pre=@scripts/ddl_div_one.sql:tgt=trafodion.odb_test.div_one:map=./odb_div_map/div_one.map'
    usecase = 'Load one column data, the value of DIV is Positive integer '
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 75, write by jianhua, the next cases are about functional of div
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/div_more:pre=@scripts/ddl_div_more.sql:tgt=trafodion.odb_test.div_more:map=./odb_div_map/div_more.map'
    usecase = 'Load multi columns data, the value of DIV is Positive integer '
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


   #test 76, write by jianhua, the next cases are about functional of div
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/div_one:pre=@scripts/ddl_div_one.sql:tgt=trafodion.odb_test.div_one:map=./odb_div_map/div_one_unlimited.map'
    usecase = 'Load one column data, the value of DIV is Unlimited integer'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 77, write by jianhua, the next cases are about functional of div
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/div_more:pre=@scripts/ddl_div_more.sql:tgt=trafodion.odb_test.div_more:map=./odb_div_map/div_more_unlimited.map'
    usecase = 'Load multi columns data, the value of DIV is Unlimited integer '
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 78, write by jianhua, the next cases are about functional of div
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/div_one_error:pre=@scripts/ddl_div_one.sql:tgt=trafodion.odb_test.div_one:map=./odb_div_map/div_one.map'
    usecase = 'Load one column data with Error data, the value of DIV is Positive integer, the expected value is failed!!!'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 79, write by jianhua, the next cases are about functional of div
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/div_more_error:pre=@scripts/ddl_div_more.sql:tgt=trafodion.odb_test.div_more:map=./odb_div_map/div_more.map'
    usecase = 'Load multi columns with Error data, the value of DIV is Positive integer, the expected value is failed!!!!'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1



    #test 80, write by jianhua, the next cases are about functional of div
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/div_one:pre=@scripts/ddl_div_one.sql:tgt=trafodion.odb_test.div_one:map=./odb_div_map/div_one_decimal.map'
    usecase = 'Load one column data, the value of DIV is a Decimal '
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 81, write by jianhua, the next cases are about functional of div
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/div_more:pre=@scripts/ddl_div_more.sql:tgt=trafodion.odb_test.div_more:map=./odb_div_map/div_more_decimal.map'
    usecase = 'Load multi columns data, the value of DIV is a Decimal '
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 82, write by jianhua, the next cases are about functional of div
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/div_one:pre=@scripts/ddl_div_one.sql:tgt=trafodion.odb_test.div_one:map=./odb_div_map/div_one_error.map'
    usecase = 'Load one column data, the value of DIV is a Error Number, The expected value of this case is Failed!!!! '
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1



    #test 83, write by jianhua, the next cases are about functional of div
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/div_more:pre=@scripts/ddl_div_more.sql:tgt=trafodion.odb_test.div_more:map=./odb_div_map/div_more_error.map'
    usecase = 'Load multi columns data, the value of DIV are Error data, The expected value of this case is Failed!!!! '
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1



    #test 84, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_one:pre=@scripts/ddl_fixed_one.sql:tgt=trafodion.odb_test.fixed_one:map=./odb_fixed_map/fixed_one_zero.map'
    usecase = 'Load one column data, the start index of DIV file is zero, length is right '
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 85, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_one:pre=@scripts/ddl_fixed_one.sql:tgt=trafodion.odb_test.fixed_one:map=./odb_fixed_map/fixed_one_unzero.map'
    usecase = 'Load one column data, the start index of DIV file is not zero, length is right '
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 86, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_one:pre=@scripts/ddl_fixed_one.sql:tgt=trafodion.odb_test.fixed_one:map=./odb_fixed_map/fixed_one_startneg.map'
    usecase = 'Load one column data, the start index of DIV file is a negative number, the result of this case is failed!!!!'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 87, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_one:pre=@scripts/ddl_fixed_one.sql:tgt=trafodion.odb_test.fixed_one:map=./odb_fixed_map/fixed_one_starterr.map'
    usecase = 'Load one column data, the start index of DIV file is a wrong number, the result of this case is failed!!!!'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 88, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_one:pre=@scripts/ddl_fixed_one.sql:tgt=trafodion.odb_test.fixed_one:map=./odb_fixed_map/fixed_one_lenerr.map'
    usecase = 'Load one column data, the length of DIV file is a wrong number, the result of this case is failed!!!!'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 89, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_one:pre=@scripts/ddl_fixed_one.sql:tgt=trafodion.odb_test.fixed_one:map=./odb_fixed_map/fixed_one_lenneg.map'
    usecase = 'Load one column data, the length of DIV file is a negative number, the result of this case is failed!!!!'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 90, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_one:pre=@scripts/ddl_fixed_one.sql:tgt=trafodion.odb_test.fixed_one:map=./odb_fixed_map/fixed_one_lenoutofrange.map'
    usecase = 'Load one column data, the length of DIV file is a number out of range, It could execute successfully'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 91, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_one_error:pre=@scripts/ddl_fixed_one.sql:tgt=trafodion.odb_test.fixed_one:map=./odb_fixed_map/fixed_one_zero.map'
    usecase = 'Load one column data with error number, the result of this testcase is failed!!!! '
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 92, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_more:pre=@scripts/ddl_fixed_more.sql:tgt=trafodion.odb_test.fixed_more:map=./odb_fixed_map/fixed_more_zero.map'
    usecase = 'Load multiple columns data, the start index of first column is zero, other columns are correct, length is right'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 93, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_more:pre=@scripts/ddl_fixed_more.sql:tgt=trafodion.odb_test.fixed_more:map=./odb_fixed_map/fixed_more_unzero.map'
    usecase = 'Load multiple columns data, the start index of first column is unzero, other columns are correct, length is right'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 94, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_more:pre=@scripts/ddl_fixed_more.sql:tgt=trafodion.odb_test.fixed_more:map=./odb_fixed_map/fixed_more_allzero.map'
    usecase = 'Load multiple columns data, the start index of all columns is zero, length is right'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 95, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_more:pre=@scripts/ddl_fixed_more.sql:tgt=trafodion.odb_test.fixed_more:map=./odb_fixed_map/fixed_more_startneg.map'
    usecase = 'Load multiple columns data, the start index of any columns is negative, length is right, the expected result of this testcase is failed!!!'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 96, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_more:pre=@scripts/ddl_fixed_more.sql:tgt=trafodion.odb_test.fixed_more:map=./odb_fixed_map/fixed_more_starterr.map'
    usecase = 'Load multiple columns data, the start index of any columns is error index, length is right, the expected result of this testcase is failed!!!'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1

    #test 97, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_more:pre=@scripts/ddl_fixed_more.sql:tgt=trafodion.odb_test.fixed_more:map=./odb_fixed_map/fixed_more_lenneg.map'
    usecase = 'Load multiple columns data, the start index is correct, length is a negative number, the expected result of this testcase is failed!!!'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 98, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_more:pre=@scripts/ddl_fixed_more.sql:tgt=trafodion.odb_test.fixed_more:map=./odb_fixed_map/fixed_more_lenerr.map'
    usecase = 'Load multiple columns data, the start index is correct, length is a error number, the expected result of this testcase is failed!!!'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1



    #test 99, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_more:pre=@scripts/ddl_fixed_more.sql:tgt=trafodion.odb_test.fixed_more:map=./odb_fixed_map/fixed_more_lenoutofrange.map'
    usecase = 'Load multiple columns data, the start index is correct, length is out of range, the expected result of this testcase is failed!!!!'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    #test 100, write by jianhua, the next cases are about functional of fixed
    total_num += 1
    cmd = preCmd + ' -u ' + username + ' -p ' + passwd + ' -d ' + database + ' -l src=./odb_auto_loadfile/fixed_more_error:pre=@scripts/ddl_fixed_more.sql:tgt=trafodion.odb_test.fixed_more:map=./odb_fixed_map/fixed_more_zero.map'
    usecase = 'Load multiple columns error data, the start index is correct, length is correct, the expected result of this testcase is failed!!!'
    ret = execute_cmd_other(total_num, usecase, cmd)
    if ret != 0:
        failed_num += 1
    else:
        success_num +=1


    '''    
    end_time = time.time()
    logger_detail.info('======================================================================================\nTOTAL : %d\nPASSED : %d\nFAILED : %d\nDURING : %ds\n',
    total_num, total_num - failed_num, failed_num, end_time - start_time)
    logger_sumary.info('======================================================================================\nTOTAL : %d\nPASSED : %d\nFAILED : %d\nDURING : %ds\n',
    total_num, total_num - failed_num, failed_num, end_time - start_time)
    '''
    end_time = time.time()
    logger_detail.info('======================================================================================\nTOTAL : %d\nPASSED : %d\nFAILED : %d\nDURING : %ds\n',
    total_num, success_num, failed_num, end_time - start_time)
    logger_sumary.info('======================================================================================\nTOTAL : %d\nPASSED : %d\nFAILED : %d\nDURING : %ds\n',
    total_num, success_num, failed_num, end_time - start_time)

create_output()
create_script()

#create date file to test load
print "now create files to test load "
print "please wait..."
sp = ['!', '@', '#', '$', '%', '^', '&', '*', ',', '.', '"', '|', 'bad']
for i in sp :
    ret = prepare_load_data(i, '"', 1000)
    if ret != 0:
        break

mutables = ['nullstring']
for i in mutables:
    ret = prepare_load_data_add(i,1000)
    if ret != 0:
        break

#create data file to test multi files load
prepare_multi_files_data()

#create tables to test extract
ret = prepare_extract_data()
if ret != 0:
    print "create tables to extract faied, exit."
    os._exit(0)

# create config file determine the list of src to be extracted
create_config_file()

#start test
print "======================================================================================"
print "now start odb test ..."
print 
main()


