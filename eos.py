#
#    Copyright (c) 2016 Gary Fisher <gary@eosweather.ca>
#
#    See the file LICENSE.txt for your full rights.
#

# Python Imports
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import array
import sys
import os
from math import *
from decimal import Decimal 
from datetime import date, datetime, time, timedelta, tzinfo
from random import randint
from gps import *
import re
import MySQLdb as mdb
from array import *
import threading
import httplib, urllib
import logging
import logging.handlers

# eos modules imports
import eosutils as eosu
import eosformulas as eosf
import eospush as eosp
import eosdebug as eosd
import eossql as eoss
import eosrelays as eosr

#Set Version
S_Version = "1.1-8"
#Set other global variables
last_recieved = ''
EosString = ""
has_db = False
LOG_FILENAME = '/var/www/logs/eos.log'

#===============================================================================
#                    Class Sentences
#===============================================================================

class Sentence:
    Wind = ""
    Rain = ""
    Pressure = ""
    Solar = ""
    Temp = ""
    Board = ""
    Tide = ""
    
#===============================================================================
#                    Class Station
#===============================================================================

class Station:
    version = ""
    theWind = {}
    Name = "EOS_Station"
    Broker_Address = ''
    Broker_Port = ''
    Broker_USN = ''
    Broker_PWD = ''
    ID = 0
    Type = 0
    Error = 0
    Latitude = 0     # D.ddd
    Longitude = 0
    Altitude = 0     #Not set by GPS
    Compass = 0    #Future Compass direction
    Magnetic = 0
    Variation = -1 
    UoM = 0  ## 0= metric  1 = Imperial --data should always be stored in metric only?
    GPS_Active = False
    LastMinute = 99
    ## Update on which minutes?
    UpdateOn = array("i",[0,15,30,45])
    UpdateReady = False
    #These set the number of records in each table buffer
    #If updating every 6 seconds and we store every 15 min then we need 150 records
    Time = 0
    Wind = 1
    Pressure = 2
    Temp = 3
    Rain = 4
    Solar = 5
    Board = 6 
    Soil = 7
    Depth = 8
    Wind_Count = 0
    Rain_Count = 0
    Temp_Count = 0
    Time_Count = 0
    Pressure_Count = 0
    Solar_Count = 0
    Location_Count = 0
    Board_Count = 0
    Soil_Count = 0
    Depth_Count = 0
    Depth_Adjust = 1
    Datum = 0
    HHWL = 0
    doUpdate = False
    Has_Fan = False
    Solar_Factor = 1
    date_time = datetime.now()
    WaitTime = 5  #Once every 5 seconds (1 sec to run routine = 6 seconds)
    WUndergroundID = ""
    WUndergroundPWD = ""
    WUndergroundCAMID = ""
    WUndergroundCAMFILE = ""
    PWS_ID = ""
    PWS_PWD = ""
    WOW_ID = ""
    WOW_KEY = ""
    App_Token = ""
    User_Key = ""
    Remote_Conn = ""
    Remote_PHP = ""
    Remote_ID = ""
    Remote_Burst = ""
    Burst_On = False
    Burst_USN = ""
    Burst_PWD = ""
    Error_Level = 1
    Msg_Count = 0
    HeatBase = 18
    CoolBase = 18
    Sun_Trigger = 300
    ReportBase = ""
    BG_Color = ""
    Alarm_Temp = False
    Alarm_Wind = False
    Alarm_Pressure = False
    Alarm_Solar = False
    Alarm_Rain = False
    Alarm_Board = False
    Alarm_Soil = False
    Alarm_Depth = False
    Alarm_Volts = 0

#===============================================================================
#                    Class NMEA
#===============================================================================

class BROKER:
    WindSpeed = -99.9
    WindDirection = -99.9
    Pressure_Rel = -99.9
    Temp_Outside = -99.9
    Solar_Rad = -99.9
    B_Volts = -99.9
    S_Volts = -99.9
    Client = ''
    Address = ''
    Port = ''
    ID = 0



#===============================================================================
#                    Class NMEA
#===============================================================================

class NMEA:
    ON = False
    GGA = False
    RMC = False
    HDT = False
    HDM = False
    VWR = False
    MWV = False
    MWD = False
    MDA = False

#===============================================================================
#                    Class EOS gathering data from EOR
#                          Define the EOS data strcuture
#===============================================================================

class EOS:
    Hours = 0
    Minutes = 0
    Seconds = 0
    Day = 0
    Month = 0
    Year = 0
    WindSpeed = 0
    High_Gust = 0
    WindSpeed_Avg = 0
    Wind_UoM = 0
    WindDirection = 0
    WindMagnetic = 0
    Variation = 0
    WindAngle = 0 # Off Bow
    WindBow = "" # L or R
    Compass = 0
    APWindDirection = 0
    APWindSpeed = 0
    WindRose = "---"
    Pressure_Abs = 0
    Altitude = 0
    Pressure_Rel = 0
    Pressure_Trend = 0
    Temp_Outside = 0
    Temp_DewPoint = 0
    Humidity_Rel = 0
    Temp_Trend = 0
    Temp_UoM = 0
    Rain_Rate = 0
    Rain_Amount = 0
    Rain_FallToday = 0
    Rain_FallYesterday = 0
    Rain_Rate_UoM = 0
    Rain_Tips = 0
    Solar_Lum = 0
    Solar_UV = 0
    Solar_Rad = 0
    Solar_RadHi = 0
    Solar_Energy = 0
    LAT = 0
    LONG = 0
    SOG = 0
    COG = 0
    UTC = ""
    GMT_Offset = -3
    B_Volts = 0
    S_Volts = 0
    C_Temp = 0
    Error = 0
    Error2 = 0
    Version = ""
    Soil_Moisture = 0
    Soil_Temp = 0
    Soil_ID = 0
    Depth = 0
    Depth_Trend = 0
    Depth_ID = 0
    Fan = 0
    wv = 0
    wg = 0
    
    

#===============================================================================
#                    Class AOS gathering data for OUTPUT to Archive
#                          Define the AOS data strcuture
#===============================================================================

class AOS:
    w_max = "---"
    w_min = "---"
    w_avg = "---"
    w_rose = "---"
    g_max = "---"
    g_min = "---"
    g_avg = "---"
    c_avg = "---"
    windrun = "---"
    t_max = "---"
    t_min = "---"
    t_avg = "---"
    d_max = "---"
    d_min = "---"
    d_avg = "---"
    h_max = "---"
    h_min = "---"
    h_avg = "---"
    windchill = "---"
    heatout = "---"
    thw = "---"
    thws = "---"
    heat_dd = "---"
    cool_dd = "---"
    r_max = "---"
    r_hrsum = "---"
    r_dsum = "---"
    r_min = "---"
    r_avg = "---"
    r_tips = "---"
    ts_sum = "---"
    a_max = "---"
    a_min = "---"
    a_avg = "---"
    p_max = "---"
    p_min = "---"
    p_avg = "---"
    p_trend = '---'
    sl_sum = "---"
    sl_avg = "---"
    su_max = "---"
    sr_avg = "---"              
    sh_max = "---" 
    sh_min = "---"
    sh_avg = "---"
    se_sum = "---"
    we_date = " "
    we_time = " "
    we_datetime = " "
    aw_date = ""
    aw_time = ""
    t = datetime(2000, 1, 1, 0, 0, 0)
    we_Interval = " "
    p_lat = "---"
    p_long = "---"
    p_cog = "---"
    p_sog = "---"
    v_source = "---"
    v_battery = "---"
    b_avg = "---"
    solarmax = "---"
    cloudy = "---"
    solar_segment = 0
    trend = "---"
    rise = "---"
    tide = "---"
    datum = "---"
    stemp = "---"
    smoisture = "---"

#===============================================================================
#                    Class EOS_READER interpret data from EOR
#                          For EOS data strcuture
#===============================================================================
                                   
class eos_reader(object):
    global last_recieved
    global has_db

    
    def __init__(self):
        ## On startup set the defaults
        last_recieved = ""
        EOS.Hours = 0
        EOS.Minutes = 0
        EOS.Seconds = 0
        EOS.Day = 0
        EOS.Month = 0
        EOS.Year = 0
        EOS.WindSpeed = 0
        EOS.High_Gust = 0
        EOS.WindSpeed_Avg = 0
        EOS.Wind_UoM = 0
        EOS.WindDirection = 0
        EOS.WindMagnetic = 0
        EOS.Variation = 0
        EOS.Compass = 0
        EOS.APWindDirection = 0
        EOS.APWindSpeed = 0
        EOS.WindRose = "---"
        EOS.Pressure_Abs = 0
        EOS.Altitude = 0
        EOS.Pressure_Rel = 0
        EOS.Pressure_Trend = 0
        EOS.Temp_Outside = 0
        EOS.Temp_DewPoint = 0
        EOS.Humidity_Rel = 0
        EOS.Temp_Trend = 0
        EOS.Temp_UoM = 0
        EOS.Rain_Rate = 0
        EOS.Rain_Amount = 0
        EOS.Rain_FallToday = 0
        EOS.Rain_FallYesterday = 0
        EOS.Rain_Rate_UoM = 0
        EOS.Rain_Tips = 0
        EOS.Solar_Lum = 0
        EOS.Solar_UV = 0
        EOS.Solar_Rad = 0
        EOS.Solar_RadHi = 0
        EOS.Solar_Energy = 0
        EOS.LAT = 0
        EOS.LONG = 0
        EOS.SOG = 0
        EOS.COG = 0
        EOS.UTC = ""
        EOS.GMT_Offset = 0       #Need to get PC GMT_Offset
        EOS.B_Volts = 0
        EOS.S_Volts = 0
        EOS.C_Temp = 0
        EOS.Error = 0
        EOS.Error2 = 0
        EOS.Soil_Temp = 0
        EOS.Soil_Moisture = 0
        EOS.Soil_ID = 0
        EOS.Depth = 0
        EOS.Depth_Trend = 0
        EOS.Depth_ID = 0
        EOS.Fan = 0
        EOS.wg = 0
        EOS.wv = 0
        
    def Up(self):
        if has_db:
            return True
        else:
            return False


def addParam(theIndex, wordKey, wordValue):
    theIndex.setdefault(wordKey,[]).append(wordValue)


def burstCam(Station):   
    CamFiles = Station.WUndergroundCAMFILE.split('/')
    CamFile = CamFiles[len(CamFiles)-1]
    
    sent,reason = eosp.burstupload(Station, "/" + CamFile, Station.WUndergroundCAMFILE)

def publish_mqtt(mqttc, pub_topic, sensor_data):
    global BROKER
    try:
        if BROKER.Address <> '':
            result, BROKER.ID =  mqttc.publish(pub_topic, sensor_data, qos=0, retain=True)
            if result == 0:
                print 'Message Sent ID:' + str(BROKER.ID) + ' --> ' + pub_topic + ' ' + str(sensor_data)
            else:
                print 'Message Error ' + str(result)
    except Exception,e:
        
        print str(e)
    
def on_connect(client, userdata, flages, rc):
    print "CONNACK recieved with code %d." % (rc)

def on_disconnect(client, userdata, rc):
    print client + ' disconnected'

    
#===============================================================================
#                    MAIN LOOP program start
#                    
#===============================================================================

def main():
    global EOS
    global BROKER
    global NMEA
    global Station
    global mqttc
    global Relays
    global Units
    global last_recieved
    global has_db
    global S_Version
    EOS_reader = eos_reader()
    stmt = eoss.stmt()
    station = eosu.station()



    
    
##    addParam(Station.theWind, 'SentenceId', 1)
##    print Station.theWind['SentenceId'][0]
##    if Station.theWind['SentenceId'][0] == 1:
##        print Station.theWind['SentenceId']
    """Set up logging files """
    eosu.log.clear(LOG_FILENAME)
    
    level = logging.INFO
    """Change this to modify logging details for all messages DEBUG/INFO"""
    
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(lineno)d -> %(message)s")
    if len(sys.argv) > 1:
        level_name = sys.argv[1]
        level = eosu.log.LEVELS.get(level_name, logging.NOTSET)

    logging.basicConfig(filename=LOG_FILENAME,level=level) 
    eos_log = logging.getLogger('eosLogger')
    if level == logging.DEBUG:
        handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=100000, backupCount=10)
    else:
        handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=20000, backupCount=10)
    
    handler.setFormatter(formatter)
    eos_log.addHandler(handler)


    time.sleep(60) 
    ##wait until services are running change to 60 for production

#===============================================================================
#                     Make connection to MySQL 
#                    
#===============================================================================
    try:
        db = mdb.connect(host= eoss.SQL.server, port = eoss.SQL.port, user= eoss.SQL.user,passwd= eoss.SQL.password, db= eoss.SQL.database)
        ##Set up a local cursor to hold data and execute statments
        cur = db.cursor(mdb.cursors.DictCursor)          
        if station.update(Station, db):
            BROKER.Client = 'EOS_Station'
            mqttc = mqtt.Client(BROKER.Client, clean_session=True) 
            if Station.Broker_Address <> '':
                
                BROKER.Address = Station.Broker_Address
                BROKER.Port = Station.Broker_Port
                
                mqttc.username_pw_set(Station.Broker_USN,Station.Broker_PWD)
                mqttc.on_dsconnect = on_disconnect
                mqttc.on_connect = on_connect
                mqttc.connect(BROKER.Address, BROKER.Port, keepalive=90)  ##broker
                mqttc.loop_start()

            dstart = datetime.now()
            a = []
            if Station.version <> S_Version:
                a.append(stmt.version(S_Version))
                eos_log.info("Upgraded eos software version - now:" + S_Version)
            else:
                eos_log.info("Running eos software version : " + S_Version)
            ##check new tables and that we have a true connection
            has_db = eoss.MSGLOG(db, a)
            a.append("Update STATION SET INT_VALUE = 0 where LABEL = 'REM_ID'")
            if len(a) > 0:
                if eoss.sqlmupdate(db,a) == False:
                    eos_log.info("Problem deleting records and updating Version/Remote Flag")
                else:
                    eosp.sendalert(db, 20, datetime.now(), .25, 0, "EOS Server Starting", Station.App_Token, Station.User_Key)
                        
                    eos_log.info('EOS Server Starting')                    
            if station.NMEAupdate(NMEA, db):
                eos_log.info("Checked NMEA")  

        else:
            has_db = False
            if len(Station.User_Key) > 0:
                 eosp.sendpushover(Station.App_Token, Station.User_Key, "EOS Server NOT Starting: Invalid setup", 1)
            eos_log.critical('Invalid setup')

    except:
        eos_log.critical('No database connection')
        if len(Station.User_Key) > 0:
             eosp.sendpushover(Station.App_Token, Station.User_Key, "EOS Server NOT Starting: No database", 1)
        has_db = False



    ##try:
        ##mqtt access
        ##mqtt.reinitialise()
        ##mqttc = mqtt.Client("EOS_Station")
        ##mqttc.connect_async('10.0.1.33','1883', keepalive=60, bind_address='')
        ##mqttc.loop_start()
        
    ##except:
    ##    eos_log.info("No mqtt broker")
    
    
    try:
#===============================================================================
#                      Program Cycle starts here
#                    
#===============================================================================

        while EOS_reader.Up():
            eos_log.debug("Starting Cycle")
            date_time = eosu.getTime(Station, EOS)

##            publish_mqtt(mqttc, Station.Name + "/Time",date_time)

            if NMEA.ON:
                Station.GPS_Active = False
                eos_log.info("Doing NMEA")
##          NMEA DATA INBOUND 
                try:
                    if NMEA.GGA:
                        if eosu.nmea.GGA(db, Station, EOS):
                            Station.GPS_Active = True
                    if NMEA.RMC:
                        if eosu.nmea.RMC(db, Station, EOS):
                            Station.GPS_Active = True
                    if NMEA.HDT:
                        Compass = eosu.nmea.HDT(db, Station)     
                    else:
                        Compass = False
                    if NMEA.HDM:
                        Magnetic = esou.nmea.HDM(db, Station)
                    else:
                        Magnetic = False
                        
                    if Compass == True and Magnetic == True:
                        Station.Variation = eosu.add_dir(Station.Compass, Station.Magnetic)
                    elif Compass == True:
                        Station.Magnetic = eosu.add_dir(Station.Compass, Station.Variation)
                except Exception,e:
                    eos_log.error('Error on NMEA Input: ' + str(e))
                    eosp.sendalert(db, 21, Station.date_time, .25, 0, "On NMEA Input: " + str(e), Station.App_Token, Station.User_Key)

##          Collect all inserts into array
            a = []
            bb = []
##          Get LOCATION
            if Station.Location_Count > 0 and Station.GPS_Active:
                eos_log.debug("Doing Location")
                try:
                    if stmt.records(db, 'LOCATION', 0, date_time) == 0:
                        a.append(stmt.location(EOS, date_time))
                        stmt.trimrecords(db, "LOCATION", Station.Location_Count)
                except Exception,e:
                    eos_log.error('On Location: ' + str(e))
                    eosp.sendalert(db, 21, Station.date_time, .25, 0, "On Location: " + str(e), Station.App_Token, Station.User_Key)
##          Get WIND            
            if Station.Wind_Count > 0:    
                try:
                    cur.execute(stmt.windfeed())
                    row = cur.fetchone()
                    if station.EOS1(row, EOS):
                        
                        if stmt.records(db, 'WIND', Station.Wind, date_time) == 0:
                            a.append(stmt.wind(EOS, Station, date_time))
                            stmt.trimrecords(db, "WIND", Station.Wind_Count)
                            eos_log.debug("Wind Captured :" + a[len(a)-1])
                            if BROKER.WindSpeed <> EOS.WindSpeed:
                                publish_mqtt(mqttc, Station.Name + "/Wind/Speed",EOS.WindSpeed)
                                BROKER.WindSpeed = EOS.WindSpeed
                            if BROKER.WindDirection <> EOS.WindDirection:
                                publish_mqtt(mqttc, Station.Name + "/Wind/Direction",EOS.WindDirection)
                                BROKER.WindDirection = EOS.WindDirection
                        if Station.Burst_On == True and EOS.WindSpeed > 1 and Sentence.Wind <> row['SENTENCE']:
                            ##responce,status,reason,message = eosp.sendBurst('WIND',row['SENTENCE'][15:], Station.Remote_ID, Station.Remote_Conn, Station.Remote_Burst)
                            bb.append('WIND:' + row['SENTENCE'][15:])
                            Sentence.Wind = row['SENTENCE']
                            eos_log.debug("Burst Wind :" + Sentence.Wind)

                except Exception,e:
                    eosp.sendalert(db, 21, Station.date_time, .25, 0, "On Wind: " + str(e), Station.App_Token, Station.User_Key)
                    eos_log.error('On Wind: ' + str(e))
                                      
##          Get PRESSURE
            if Station.Pressure_Count > 0:
                try:
                    cur.execute(stmt.pressurefeed())
                    row = cur.fetchone()
                    if station.EOS2(row, EOS):
                        EOS.Pressure_Rel = round(eosu.pressure.Altimeter(EOS.Pressure_Abs, Station.Altitude),1)                        
                        if stmt.records(db, 'PRESSURE', Station.Pressure, date_time) == 0:
                                a.append(stmt.pressure(EOS, date_time))
                                stmt.trimrecords(db, "PRESSURE", Station.Pressure_Count)
                                eos_log.debug("Pressure Captured :" + a[len(a)-1])
                                if BROKER.Pressure_Rel <> EOS.Pressure_Rel:
                                    publish_mqtt(mqttc, Station.Name + "/Pressure/Relative",EOS.Pressure_Rel)
                                    BROKER.Pressure_Rel = EOS.Pressure_Rel
                        if Station.Burst_On == True and Sentence.Pressure <> row['SENTENCE']:
                            ##responce,status,reason,message = eosp.sendBurst('PRESSURE',row['SENTENCE'][15:], Station.Remote_ID, Station.Remote_Conn, Station.Remote_Burst)
                            bb.append('PRESSURE:' + row['SENTENCE'][15:])
                            Sentence.Pressure = row['SENTENCE']
                            eos_log.debug("Burst Pressure :" + Sentence.Pressure)

                except Exception,e:
                    eos_log.error('On Presure: ' + str(e))
                    eosp.sendalert(db, 21, Station.date_time, .25, 0, "On Pressure: " + str(e), Station.App_Token, Station.User_Key)

##          Get TEMP
            if Station.Temp_Count > 0:
                try:                    
                    cur.execute(stmt.tempfeed())
                    row = cur.fetchone()
                    if station.EOS3(row, EOS):
                        if stmt.records(db, 'TEMP', Station.Temp, date_time) == 0:
                                a.append(stmt.temp(EOS, date_time))
                                stmt.trimrecords(db, "TEMP", Station.Temp_Count)
                                eos_log.debug("Temp Captured :" + a[len(a)-1])
                                if BROKER.Temp_Outside <> EOS.Temp_Outside:
                                    publish_mqtt(mqttc, Station.Name + "/Temp/Outside",EOS.Temp_Outside)
                                    BROKER.Temp_Outside = EOS.Temp_Outside
                        if Station.Burst_On == True and Sentence.Temp <> row['SENTENCE']:
                            ##responce,status,reason,message = eosp.sendBurst('TEMP',row['SENTENCE'][15:], Station.Remote_ID, Station.Remote_Conn, Station.Remote_Burst)
                            bb.append('TEMP:' + row['SENTENCE'][15:])
                            Sentence.Temp = row['SENTENCE']
                            eos_log.debug("Burst Temp :" + Sentence.Temp)

                except Exception,e:
                    eos_log.error('On Temp: ' + str(e))
                    eosp.sendalert(db, 21, Station.date_time, .25, 0, "On Temp: " + str(e), Station.App_Token, Station.User_Key)

##          Get RAIN
            if Station.Rain_Count > 0:
                try: 
                    cur.execute(stmt.rainfeed())
                    row = cur.fetchone()
                    if station.EOS4(row, EOS):
                        if Station.Burst_On == True and Sentence.Rain <> row['SENTENCE']:
                            ##responce,status,reason,message = eosp.sendBurst('RAIN',row['SENTENCE'][15:], Station.Remote_ID, Station.Remote_Conn, Station.Remote_Burst)
                            bb.append('RAIN:' + row['SENTENCE'][15:])
                            Sentence.Rain = row['SENTENCE']
                            eos_log.debug("Burst Rain :" + Sentence.Rain)

                        if stmt.records(db, 'RAIN', Station.Rain, date_time) == 0:
                                cur.execute("SELECT ifnull(round(FALL_TODAY,1),0) M from RAIN order by W_TIME desc limit 0,1")
                                row = cur.fetchone()
                                if row is not None:
                                    if row["M"] > 0:
                                        EOS.Rain_Amount = round(EOS.Rain_FallToday - row["M"],1)
                                        if EOS.Rain_Amount < 0:
                                            EOS.Rain_Amount = 0
                                            EOS.Rain_Rate = 0
                                    else:
                                        EOS.Rain_Amount = 0
                                        EOS.Rain_Rate = 0
                                a.append(stmt.rain(EOS, date_time))
                                stmt.trimrecords(db, "RAIN", Station.Rain_Count)
                                eos_log.debug("Rain Captured :" + a[len(a)-1])
                        if Station.Alarm_Rain:
                            if eosp.sendalarm(db, 6, Station.date_time, .25, EOS.Rain_Rate, Station.App_Token, Station.User_Key):
                                eos_log.info("Rain alarm set")
                except Exception,e:
                    eos_log.error('On Rain: ' + str(e))
                    eosp.sendalert(db, 21, Station.date_time, .25, 0, "On Rain: " + str(e), Station.App_Token, Station.User_Key)

##          Get SOLAR
            if Station.Solar_Count > 0:
                try:
                    cur.execute(stmt.solarfeed())
                    row = cur.fetchone()
                    if station.EOS5(row, EOS, Station.Solar_Factor):
                        if stmt.records(db, 'SOLAR', Station.Solar, date_time) == 0:
                                a.append(stmt.solar(EOS, date_time))
                                stmt.trimrecords(db, "SOLAR", Station.Solar_Count)
                                eos_log.debug("Solar Captured :" + a[len(a)-1])
                                if BROKER.Solar_Rad <> EOS.Solar_Rad:
                                    publish_mqtt(mqttc, Station.Name + "/Solar/Radiation",EOS.Solar_Rad)
                                    BROKER.Solar_Rad = EOS.Solar_Rad
                        if eosp.sendalarm(db, 5, Station.date_time, 1, EOS.Solar_UV, Station.App_Token, Station.User_Key):
                            eos_log.info("Solar alarm set")
                        if Station.Burst_On == True and Sentence.Solar <> row['SENTENCE']:
                            ##responce,status,reason,message = eosp.sendBurst('SOLAR',row['SENTENCE'][15:], Station.Remote_ID, Station.Remote_Conn, Station.Remote_Burst)
                            bb.append('SOLAR:' + row['SENTENCE'][15:])
                            Sentence.Solar = row['SENTENCE']
                            eos_log.debug("Burst Solar :" + Sentence.Solar)
                except Exception,e:
                    eos_log.error('On Solar: ' + str(e))
                    eosp.sendalert(db, 21, Station.date_time, .25, 0, "On Solar: " + str(e), Station.App_Token, Station.User_Key)

##          Get BOARD
            if Station.Board_Count > 0:
                try:
                    cur.execute(stmt.boardfeed())
                    row = cur.fetchone()
                    if station.EOS6(row, EOS):
                        if stmt.records(db, 'BOARD', Station.Board, date_time) == 0:
                                a.append(stmt.board(EOS, Station, date_time))
                                stmt.trimrecords(db, "BOARD", Station.Board_Count)
                                eos_log.debug("Board Captured :" + a[len(a)-1])
                                if BROKER.B_Volts <> EOS.B_Volts:
                                    publish_mqtt(mqttc, Station.Name + "/Board/Battery",EOS.B_Volts)
                                    BROKER.B_Volts = EOS.B_Volts
                                if BROKER.S_VOLTS <> EOS.S_Volts:
                                    publish_mqtt(mqttc, Station.Name + "/Board/Source",EOS.S_Volts)
                                    BROKER.S_Volts = EOS.S_Volts
                        if Station.Burst_On == True and Sentence.Board <> row['SENTENCE']:
                            ##responce,status,reason,message = eosp.sendBurst('BOARD',row['SENTENCE'][15:], Station.Remote_ID, Station.Remote_Conn, Station.Remote_Burst)
                            bb.append('BOARD:' + row['SENTENCE'][15:])
                            Sentence.Board = row['SENTENCE']
                            eos_log.debug("Burst Board :" + Sentence.Board)
                except Exception,e:
                    eos_log.error('On Station: ' + str(e))
                    eosp.sendalert(db, 21, Station.date_time, .25, 0, "On Board: " + str(e), Station.App_Token, Station.User_Key)

##          Get SOIL
            if Station.Soil_Count > 0:
                try:
                    cur.execute(stmt.soilfeed())
                    row = cur.fetchone()
                    if station.EOS7(row, EOS):
                        if stmt.records(db, 'SOIL', Station.Soil, date_time) == 0:
                                a.append(stmt.soil(EOS, date_time))
                                stmt.trimrecords(db, "SOIL", Station.Soil_Count)
                                eos_log.debug("Soil Captured :" + a[len(a)-1])
                except Exception,e:
                    eos_log.error('On Station: ' + str(e))
                    eosp.sendalert(db, 21, Station.date_time, .25, 0, "On Soil: " + str(e), Station.App_Token, Station.User_Key)

##          Get DEPTH
            if Station.Depth_Count > 0:
                try:                    
                    cur.execute(stmt.depthfeed())
                    row = cur.fetchone()
                    if station.EOS8(row, EOS):
                        if stmt.records(db, 'DEPTH', Station.Depth, date_time) == 0:
                            EOS.Depth = (EOS.Depth * Station.Depth_Adjust) + Station.Datum
                            ##fix for bad data when sensor is exposed to air
                            if EOS.Depth > Station.HHWL * 2:
                                EOS.Depth = 0
                            a.append(stmt.depth(EOS, date_time))
                            stmt.trimrecords(db, "DEPTH", Station.Depth_Count)
                            eos_log.debug("Depth Captured :" + a[len(a)-1])
                        if Station.Burst_On == True and Sentence.Tide <> row['SENTENCE']:
                            ##responce,status,reason,message = eosp.sendBurst('TIDE',row['SENTENCE'][15:] + str(Station.Datum) + "-" + str(Station.Depth_Adjust), Station.Remote_ID, Station.Remote_Conn, Station.Remote_Burst)
                            bb.append('DEPTH:' + row['SENTENCE'][15:] + str(Station.Datum) + "-" + str(Station.Depth_Adjust))
                            Sentence.Tide = row['SENTENCE']
                            eos_log.debug("Burst Tide :" + Sentence.Tide)
                        if Station.Alarm_Depth:

                            if eosp.sendalarm(db, 10, Station.date_time, .25, Station.Datum - EOS.Depth, Station.App_Token, Station.User_Key):
                                eos_log.info("Depth alarm set")
                except Exception,e:
                    eos_log.error('On Depth: ' + str(e))
                    eosp.sendalert(db, 21, Station.date_time, .25, 0, "On Depth: " + str(e), Station.App_Token, Station.User_Key)

#===============================================================================
#                      INSERT ALL RECORDS
#                      Data has been collected into a set of SQL statments
#===============================================================================
            if len(a) >0:                        
                if eoss.sqlmupdate(db,a) == False:
                        eos_log.error("Failed to insert TABLE records")
            if len(bb) > 0:
                responce,status,reason,message = eosp.sendBurst(bb , Station.Remote_ID, Station.Remote_Conn, Station.Remote_Burst)

#===============================================================================
#                      NMEA DATA OUTBOUND
#                      
#===============================================================================
        
            a = []
            if station.NMEAupdate(NMEA,db):
                if NMEA.ON:
                    u = datetime.utcnow()
                    utc = datetime(u.year, u.month, u.day, u.hour, u.minute, 0)
                    ## remove old data in NMEA
                    cur.execute("DELETE FROM NMEA WHERE WE_Date_Time < '" + utc.strftime("%Y-%m-%d %H:%M:%S") + "'")
                    db.commit()
                    try:
                        if NMEA.MDA:
                            a.append(stmt.mda(EOS, u))
                            eos_log.debug("NMEA MDA Captured :" + a[len(a)-1])
                        if NMEA.MWV:
                            a.append(stmt.mvw(EOS, u))
                            eos_log.debug("NMEA MWA Captured :" + a[len(a)-1])
                        if NMEA.MWD:
                            a.append(stmt.mwd(EOS, u))
                            eos_log.debug("NMEA MWD Captured :" + a[len(a)-1])
                        if NMEA.VWR:
                            a.append(stmt.vwr(EOS, u))
                            eos_log.debug("NMEA VWR Captured :" + a[len(a)-1])
                    except Exception,e:
                        eos_log.error('On NMEA output: ' + str(e))
                        eosp.sendalert(db, 21, Station.date_time, .25, 0, "On NMEA out: " + str(e), Station.App_Token, Station.User_Key)
            else:
                Station.GPS_Active = False
                EOS.LAT = Station.Latitude
                EOS.LONG = Station.Longitude
                EOS.SOG = 0
                EOS.COG = Station.Compass

##          INSERT ALL NMEA Records
            if len(a) >0:                        
                if eoss.sqlmupdate(db,a) == False:
                    eos_log.error("Failed to insert NMEA records")
                else:
                    eos_log.debug("Inserted NMEA records")

#===============================================================================
#                      DONE
#                      UPDATE STATION DEFAULTS ONCE A MINUTE
#===============================================================================         
            if Station.LastMinute <> EOS.Minutes:
               
               a = []
               Station.LastMinute = EOS.Minutes
               c2 = 0
##              DO UPDATE every 30 minutes to send signal to Hardware
               if EOS.Minutes == 5 or EOS.Minutes == 35:
                   a.append("UPDATE STATION SET INT_VALUE = 4 where LABEL = 'DO_UPDATE'")

##             DO UPDATE ON EOS at 12:00am each day
               if EOS.Hours == 0 and EOS.Minutes == 0:
                   a.append("UPDATE STATION SET INT_VALUE = 1 where LABEL = 'DO_UPDATE'")
               ##get any new settings
               if station.update(Station, db):
                    eos_log.debug("Retrieved any new Station values")
                    if Station.GPS_Active == True:
                        a.append("UPDATE STATION SET INT_VALUE = 1 where LABEL = 'GPS_ACTIVE'")
                        a.append("UPDATE STATION SET STR_VALUE = '" + str(Station.Latitude) + "' WHERE LABEL = 'LATITUDE'")
                        a.append("UPDATE STATION SET STR_VALUE = '" + str(Station.Longitude) + "' where LABEL = 'LONGITUDE'")
                    else:
                        a.append("UPDATE STATION SET INT_VALUE = 0 where LABEL = 'GPS_ACTIVE'")
                    if Station.doUpdate == True:
                        a.append("UPDATE STATION SET INT_VALUE = 1 where LABEL = 'DO_UPDATE'")
                        eos_log.debug("Forcing EOR to perform update")
                        Station.doUpdate = False
               else:
                    has_db = False
                    eos_log.error('Archive Update Database Issue: ' + str(e))
                    eosp.sendalert(db, 21, Station.date_time, .25, 0, "On Update Archive: " + str(e), Station.App_Token, Station.User_Key)
               if len(a) > 0 :
                    if eoss.sqlmupdate(db,a) == False:
                        eos_log.error("Failed to update records :" + a)

#===============================================================================
#                      DO ARCHIVE DATA ON ARCH CYCLE
#                      Gather archive interval data
#===============================================================================            
               for t in Station.UpdateOn:
                    c2 = c2 + 1
                    if t == Station.LastMinute:
                        c1 = c2
                        dend = datetime(2000 + int(EOS.Year), int(EOS.Month), int(EOS.Day), int(EOS.Hours), int(Station.LastMinute), 0)
                        sunrise = datetime(2000 + int(EOS.Year), int(EOS.Month), int(EOS.Day), 0, 0, 0)
                        dInt = Station.UpdateOn[1] - Station.UpdateOn[0]
                        dstart = dend - timedelta(minutes= dInt)
                        rstart = dend - timedelta(minutes= 60) ##for rain in last hour
                        eos_log.info('Preparing to archive at ' + dend.isoformat())
                        station.clearAOS(AOS)
## get wind archive                        
                        try:
                            if Station.Wind_Count >0:
                                stmt.windarch(AOS, dstart, dInt, db)
                                eos_log.debug("Wind Archive completed")          
                        except Exception,e:
                            eos_log.error('Archive: Wind : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On Archive Wind: " + str(e), Station.App_Token, Station.User_Key)                            
## get temp archive                         
                        try:
                            if Station.Temp_Count > 0:
                                stmt.temparch(AOS, dstart, dInt, Station, db)
                                publish_mqtt(mqttc, Station.Name + "/Temp/OutsideAverage", AOS.t_avg)
                                eos_log.debug("Temp Archive completed")                            
                        except Exception,e:
                            eos_log.error('Archive: Temp : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On Archive Temp: " + str(e), Station.App_Token, Station.User_Key)
## get rain archive
                        try:
                            if Station.Rain_Count > 0:                               
                                stmt.rainarch(AOS, dstart, db)
                                eos_log.debug("Rain Archive completed")
                        except Exception,e:
                            eos_log.error('Archive: Rain : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On Archive Rain: " + str(e), Station.App_Token, Station.User_Key)
## get pressure archive                                
                        try:
                            if Station.Pressure_Count > 0:
                                stmt.pressurearch(AOS, dstart, db)
                                eos_log.debug("Pressure Archive completed")
                                publish_mqtt(mqttc, Station.Name + "/Pressure/RelativeAverage", AOS.p_avg)
                        except Exception,e:
                            eos_log.error('Archive: Pressure : ' + str(e))
                            eosp.sendalert(db, 21, Station.date_time, .25, 0, "On Archive Pressure: " + str(e), Station.App_Token, Station.User_Key)
## get solar archive
                        try:
                            if Station.Solar_Count > 0:
                                stmt.solararch(AOS, dstart, dend, sunrise, dInt, db)
                                eos_log.debug("Solar Archive completed")
                                publish_mqtt(mqttc, Station.Name + "/Solar/RadiationAverage", AOS.sr_avg)
                        except Exception,e:
                            eos_log.error('Archive: Solar : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On Archive Solar: " + str(e), Station.App_Token, Station.User_Key)                                 
## get board archive
                        try:
                            if Station.Board_Count > 0:
                                stmt.boardarch(AOS, dstart, db)
                                eos_log.debug("Board Archive completed")
                        except Exception,e:
                            eos_log.error('Archive: Board : ' + str(e))
                            if len(Station.User_Key) > 0:
                                eosp.sendpushover(Station.App_Token, Station.User_Key, "On Archive Board: " + str(e), 3)
## get location
                        try:
                            #location is always current state                            
                            AOS.p_lat = Station.Latitude
                            AOS.p_long = Station.Longitude
                            AOS.p_cog = "{0:.0f}".format(EOS.COG)
                            AOS.p_sog = "{0:.1f}".format(EOS.SOG)
                        except Exception,e:
                            eos_log.error('Archive Location : ' + str(e))
                            if len(Station.User_Key) > 0:
                                eosp.sendpushover(Station.App_Token, Station.User_Key, "On Archive Location: " + str(e), 3)
## get time details
                        try:
                            u = datetime.utcnow()
                            utc = datetime(u.year, u.month, u.day, u.hour, u.minute, 0)
                            AOS.we_date = dend.strftime("%Y-%m-%d")
                            AOS.we_time = dend.strftime("%H:%M:%S")
                            AOS.aw_date = dend.strftime("%d.%m.%Y")
                            AOS.aw_time = dend.strftime("%H:%M")
                            AOS.we_datetime = utc.strftime("%Y-%m-%d %H:%M:%S")
                            t = datetime(2000 + int(EOS.Year), int(EOS.Month), int(EOS.Day), 0, int(dInt), 0)
                            AOS.we_Interval = t.strftime("%H:%M:%S")
                        except Exception,e:
                            eos_log.error('Archive Time : ' + str(e))
                            if len(Station.User_Key) > 0:
                                eosp.sendpushover(Station.App_Token, Station.User_Key, "On Archive Time " + str(e), 3)                      
## Build archive insert for core data
                        try:
                            aa = []
                            aa.append(stmt.core_data(AOS))
                            eos_log.debug("AOS Archive record -" + aa[len(aa)-1])
                        except Exception,e:
                            eos_log.error('Inserting Archive data : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "Inserting Archive data: " + str(e), Station.App_Token, Station.User_Key)
## Build archive insert for extended data
                        try:
                            if Station.Board_Count > 0 or Station.Location_Count > 0:
                                aa.append(stmt.core_ext(AOS))
                                eos_log.debug("AOS Archive extension:" + aa[len(aa)-1])
                        except Exception,e:
                            eos_log.error('Inserting extended Archive data : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On extended Archive data: " + str(e), Station.App_Token, Station.User_Key)
## Get daily rain amounts
                        try:        
                            if Station.Rain_Count > 0:
                                eos_log.debug("Getting daily rain amount")
                                cur.execute("SELECT SUM(RAIN) R_TOTAL FROM CORE_DATA WHERE WE_DATE = '" + AOS.we_date + "'")
                                for row in cur.fetchall():
                                    if row is not None:
                                        rt = row["R_TOTAL"]
                                        publish_mqtt(mqttc, Station.Name + "/Rain/Today", rt)
                                        if isinstance(rt,float):
                                            AOS.r_dsum = str("{0:.1f}".format(round(row["R_TOTAL"],1)))
                                        else:
                                            AOS.r_dsum = '0.0'
                                AOS.r_hrsum = AOS.r_max
                            else:
                                AOS.r_dsum = '---'
                                AOS.r_hrsum = '---'

                        except Exception,e:
                            eos_log.error('Inserting daily raim Archive data : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On daily rain Archive data: " + str(e), Station.App_Token, Station.User_Key)

##Get archive soil data
                        try:
                            if Station.Soil_Count >0:
                                a = stmt.soilarch(AOS, dstart, db)
                                
                                if a <> '':
                                    aa.append("INSERT INTO SOIL_DATA VALUES " + a)
                                    eos_log.debug("Soil Archive completed")
                                    
                        except Exception,e:
                            eos_log.error('Inserting Soil Archive data : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On Soil Archive data: " + str(e), Station.App_Token, Station.User_Key)

##Get archive depth data
                        try:
                            if Station.Depth_Count >0:

                                a = stmt.deptharch(Station.Datum, AOS, dstart, db)
                                if a <> '':
                                    aa.append("INSERT INTO DEPTH_DATA VALUES " + a)
                                    eos_log.debug("Depth Archive completed")

                        except Exception,e:
                            eos_log.error('Inserting Depth Archive data : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On Depth Archive data: " + str(e), Station.App_Token, Station.User_Key)                            


#===============================================================================
#                      INSERT ARCHIVE RECORDS
#                      
#===============================================================================
                        try:
                            if len(aa) > 0:
                                if eoss.sqlmupdate(db,aa) == False:
                                    eos_log.info("Failed to insert ARCHIVE records")
                                else:
                                    eos_log.debug("Archive records inserted")
                                    if Station.Depth_Count >0:
                                        cur.execute("Select * from DEPTH_RISE_DATA where DEPTH_ID = 1 order by WE_DATE_TIME desc LIMIT 0,1")
                                        for row in cur.fetchall():
                                            AOS.trend = row["TREND"]
                                            rt = row["RISE"]
                                            if isinstance(rt,float):
                                                AOS.rise = str("{0:.1f}".format(round(row["RISE"],1)))
                                            else:
                                                AOS.rise = '---'
                                            rt = row["TIDE"]
                                            if isinstance(rt,float):
                                                AOS.tide = str("{0:.1f}".format(round(row["TIDE"],1)))
                                            else:
                                                AOS.tide = '---'
                                            rt = row["DATUM"]
                                            if isinstance(rt,float):
                                                AOS.datum = str("{0:.1f}".format(round(row["DATUM"],1)))
                                            else:
                                                AOS.datum = '---'

                                    if Station.Soil_Count >0:
                                        cur.execute("Select * from SOIL_DATA where SOIL_ID = 1 order by WE_DATE_TIME desc LIMIT 0,1")
                                        for row in cur.fetchall():
                                            AOS.stemp = row["TEMP"]
                                            AOS.smoisture = row["MOISTURE"]
                        except Exception,e:
                            eos_log.error('Inserting Archive records : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On Archive : " + str(e), Station.App_Token, Station.User_Key)  

#===============================================================================
#                      DO REMOTE PUSH(s)
#                      
#===============================================================================
                        try:
                            eos_log.info('Broker messages sent :' + str(BROKER.ID))
                            time.sleep(randint(5,15))
                            responce,status,reason,message = eosp.sendremote(AOS, Station.Remote_ID, Station.Remote_Conn, Station.Remote_PHP)
                            a = []
                            if responce == True:
                                if status == 200:
                                    eos_log.info("Remote data sent to " + Station.Remote_Conn + Station.Remote_PHP)
                                    ##Do Camera
                                    if len(Station.WUndergroundCAMFILE) > 0 and len(Station.Burst_USN) > 0:
                                        burstCam(Station)   
                                    cur.execute("Select * from MSGLOG where DONE = 0 and MSGTYPE = 1")
                                    for row in cur.fetchall():
                                        n = row["WE_DATE_TIME"]
                                        rowdate = re.sub("T"," ",n.isoformat())
                                        responce, status, reason, message = eosp.resendremote(row["MSG"], Station.Remote_Conn, Station.Remote_PHP)
                                        if responce == True:
                                            if status == 200:
                                                eos_log.info("Remote data sent to " + Station.Remote_Conn + Station.Remote_PHP + " for " + rowdate)
                                                reply = reason
                                            else:
                                                eos_log.error("Remote data not sent to " + Station.Remote_Conn + Station.Remote_PHP + " for " + rowdate)
                                                reply = "BAD"
                                        else:
                                            eos_log.error("Remote data not sent to " + Station.Remote_Conn + Station.Remote_PHP + " for " + rowdate)
                                            reply = "BAD"
                                        cur2 = db.cursor()
                                        cur2.execute("UPDATE MSGLOG SET DONE = 1, REPLY = '" + reply + "', SENT = '" + AOS.we_datetime + "' where WE_DATE_TIME = '" + rowdate + "' AND MSGTYPE = 1")
                                        db.commit()
                                        cur2.close()
                                    a.append("Update STATION SET INT_VALUE = 1 where LABEL = 'REM_ID'")
                                    a.append("DELETE FROM MSGLOG where DONE = 1 and MSGTYPE = 1")      
                                else:
                                    eos_log.info("Remote data not recieved:" + str(status) + " ->" + reason)
                                    a.append("INSERT INTO MSGLOG VALUES('" +  AOS.we_datetime + "',1,0,'" + message + "','" + str(status) + ":" + reason + "','" + AOS.we_datetime + "')")
                                    a.append("Update STATION SET INT_VALUE = 0 where LABEL = 'REM_ID'")
                            else:
                                if len(message) >0:
                                    eos_log.error("Remote send has error:" + reason)
                                    a.append("INSERT INTO MSGLOG VALUES('" +  AOS.we_datetime + "',1,0,'" + message + "','" + reason + "','" + AOS.we_datetime + "')")
                                    a.append("Update STATION SET INT_VALUE = 0 where LABEL = 'REM_ID'")
                                else:
                                    eos_log.error("Remote data not sent, no message returned")
                                    a.append("Update STATION SET INT_VALUE = 0 where LABEL = 'REM_ID'")
                            if len(a) > 0:
                                if eoss.sqlmupdate(db,a) == False:
                                    eos_log.error("Failed to insert MSGLOG records")
                                else:
                                    eos_log.debug("Inserted MSGLOG :" + a[len(a)-1])
                        except Exception,e:
                            eos_log.error('Sending Archive data : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On sending archive data: " + str(e), Station.App_Token, Station.User_Key)

                        try:
                            responce,status,reason,message = eosp.sendpws(AOS, Station.PWS_ID, Station.PWS_PWD)
                            if responce == True:
                                if status == 200:
                                    eos_log.debug("PWS data sent:" + str(status) + " ->" + reason)
                                else:
                                    eos_log.error("PWS data NOT recieved:" + str(status) + " -->" + reason)
                            else:
                                if len(message) > 0:
                                    eos_log.error("PWS send has error:" + reason)
                                else:
                                    eos_log.debug("PWS data not sent")
                        except Exception,e:
                            eos_log.error("On sending PWD data:" + str(e))

                        try:
                            responce,status,reason,message = eosp.sendwow(AOS, Station.WOW_ID, Station.WOW_KEY)
                            if responce == True:
                                if status == 200:
                                    eos_log.debug("WOW data sent:" + str(status) + " ->" + reason)
                                else:
                                    eos_log.error("WOW data NOT recieved:" + str(status) + " -->" + reason)
                            else:
                                if len(message) > 0:
                                    eos_log.error("WOW send has error:" + reason)
                                else:
                                    eos_log.debug("WOW data not sent")
                        except Exception,e:
                            eos_log.error("On sending WOW data:" + str(e))

                        try:   
                            responce,status,reason,message = eosp.sendwunderground(AOS, Station.WUndergroundID, Station.WUndergroundPWD)
                            a = []
                            if responce == True:
                                if status == 200:
                                    eos_log.info("WUnderGround data sent:" + str(status) + " ->" + reason)
                                    cur.execute("Select * from MSGLOG where DONE = 0 and MSGTYPE = 2")
                                    for row in cur.fetchall():
                                        n = row["WE_DATE_TIME"]
                                        rowdate = re.sub("T"," ",n.isoformat())
                                        responce, status, reason, message = eosp.resendwunderground(row["MSG"])
                                        if responce == True:
                                            if status == 200:
                                                eos_log.info("Remote data sent to WUnderGround for " + rowdate)
                                                reply = reason
                                            else:
                                                eos_log.error("Remote data not sent to WUnderGround for " + rowdate)
                                                reply = "BAD"
                                        else:
                                            eos_log.info("Remote data WUnderGround for " + rowdate)
                                            reply = "BAD"
                                        cur2 = db.cursor()
                                        cur2.execute("UPDATE MSGLOG SET DONE = 1, REPLY = '" + reply + "', SENT = '" + AOS.we_datetime + "' where WE_DATE_TIME = '" + rowdate + "' AND MSGTYPE = 2")
                                        db.commit()
                                        cur2.close()
                                    a.append("DELETE FROM MSGLOG where DONE= 1 and MSGTYPE = 2")
                                else:
                                    eos_log.error("WUnderGround data not recieved:" + str(status) + " ->" + reason)
                                    if status <> 999:
                                        a.append("INSERT INTO MSGLOG VALUES('" +  AOS.we_datetime + "',2,0,'" + message + "','" + str(status) + ":" + reason + "','" + AOS.we_datetime + "')")
                                        eos_log.debug("Saved WUnderGround: " + a[len(a)-1])
                                                    
                            else:
                                if len(message) > 0:
                                    eos_log.error("WUnderGround send has error:" + reason)
                                    a.append("INSERT INTO MSGLOG VALUES('" +  AOS.we_datetime + "',2,0,'" + message + "','" + reason + "','" + AOS.we_datetime + "')")
                                    eos_log.debug("Saved WUnderGround: " + a[len(a)-1])
                                else:
                                    eos_log.debug("WUnderGround data not sent")
                            if len(a) > 0:
                                if eoss.sqlmupdate(db,a) == False:
                                    eos_log.error("Failed to insert MSGLOG records")
                                    
                        except Exception,e:
                            eos_log.error('On sending WUnderGround data : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On sending WUnderGround data: " + str(e), Station.App_Token, Station.User_Key)
                        try:    
                            responce, reason = eosp.camupload(Station.WUndergroundCAMID, Station.WUndergroundPWD, Station.WUndergroundCAMFILE)
                            if responce == True:
                                eos_log.info("WUnderGround CAM sent")
                            else:
                                eos_log.error("WUnderCAM NOT sent: ->" + reason)
                        except Exception,e:
                            eos_log.error('On sending WUnderCAM: ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On sending WUnderCAM: " + str(e), Station.App_Token, Station.User_Key)

                        try:    
                            if eosp.sendawekas(AOS, Station.ReportBase):
                                eos_log.info("AWEKAS file created")
                            else:
                                eos_log.error("AWEKAS file not created")
                        except Exception,e:
                            eos_log.error('On making AWEKAS data : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On making AWEKAS data: " + str(e), Station.App_Token, Station.User_Key)

#===============================================================================
#                       Clean out FEEDS prior to start time of this interval
#                      
#===============================================================================                            
                        before, after = stmt.feedtrim(db, dstart)
                        if before == after:
                            eos_log.error("Failed to delete FEED records before " + dstart.isoformat() + "  count :" + str(before) )
                        else:
                            eos_log.info("Removed eor feed data before " + dstart.isoformat() + " Before count : " + str(before) + " after: " + str(after))
                        if Station.Burst_On == True:
                            responce, message = eosp.burstupload(Station, "/log.txt", LOG_FILENAME)
                            if responce == True:
                                eos_log.debug("Log file uploaded")
                            else:
                                eos_log.error("Log file upload failed : " + message)
                        if after > 1000:
                            eos_log.critical("Feed is above limit" + str(after))
                            eosp.sendalert(db, 22, Station.date_time, .25, 1, "EOS is not clearing records : " + str(after), Station.App_Token, Station.User_Key)

#===============================================================================
#                     Do alarms for achive level or delete any remaining records
#                      
#===============================================================================
                        try:
                            a = []
                            if Station.Wind_Count> 0:
                                if Station.Alarm_Wind:
                                    if eosp.sendalarm(db, 1, dend, 4, AOS.w_max, Station.App_Token, Station.User_Key):                                        
                                        eos_log.info("Wind speed alarm set")
                                #Turn fan on/off via EOR script if we have wind and solar wind < 5kps and sun > 300 
                                if Station.Has_Fan:
                                    if float(AOS.w_avg) < 5 and float(AOS.sh_max) > 300:
                                        #Check if fan is not running and turn on
                                        if EOS.Fan == 0:
                                            a.append("UPDATE STATION set INT_VALUE = 3 where LABEL = 'DO_UPDATE'")
                                            eos_log.info("Turning fan ON")
                                    else:
                                        #check if fan is running and turn off
                                        if EOS.Fan == 1:
                                            a.append("UPDATE STATION set INT_VALUE = 2 where LABEL = 'DO_UPDATE'")
                                            eos_log.info("Turning fan OFF")
                            else:
                                a.append(stmt.delete(Station.Wind))
                                a.append("DELETE from WIND")
                                eos_log.debug("Will clear WIND")
                            if Station.Pressure_Count > 0:
                                if Station.Alarm_Pressure:
                                    if (EOS.Pressure_Trend == 4 or EOS.Pressure_Trend == 5):
                                        if eosp.sendalarm(db, 7, Station.date_time, 4, AOS.p_avg, Station.App_Token, Station.User_Key):
                                            eos_log.info("Pressure drop alarm sent")
                            else:
                                a.append(stmt.delete(Station.Pressure))
                                a.append("DELETE from PRESSURE")
                                eos_log.debug("Will clear PRESSURE")
                            if Station.Temp_Count > 0:
                                if Station.Alarm_Temp:
                                    if eosp.sendalarm(db, 2, dend, 4, AOS.heatout, Station.App_Token, Station.User_Key):
                                        eos_log.info("Heat Index alarm sent")
                                    if eosp.sendalarm(db, 3, dend, 4, AOS.thw, Station.App_Token, Station.User_Key):
                                        eos_log.info("THW alarm sent")
                                    if eosp.sendalarm(db, 4, dend, 4, AOS.thws, Station.App_Token, Station.User_Key):
                                        eos_log.info("THWS alarm sent")
                                    if eosp.sendalarm(db, 8, dend, 4, AOS.t_avg, Station.App_Token, Station.User_Key):
                                        eos_log.info("Temperature alarm sent")
                            else:
                                a.append(stmt.delete(Station.Temp))
                                a.append("DELETE from TEMP")
                                eos_log.debug("Will clear TEMP")
                            if Station.Rain_Count == 0:
                                a.append(stmt.delete(Station.Rain))
                                a.append("DELETE from RAIN")
                                eos_log.debug("Will clear RAIN")
                            if Station.Solar_Count ==0:
                                a.append(stmt.delete(Station.Solar))
                                a.append("DELETE from SOLAR")
                                eos_log.debug("Will clear SOLAR")
                            if Station.Board_Count > 0:
                                if Station.Alarm_Board:
                                    if Station.Alarm_Volts > 0 and AOS.v_battery <> '---':
                                        if float(AOS.v_battery) <= Station.Alarm_Volts:
                                            if eosp.sendalarm(db, 9, dend, 4 , AOS.v_battery , Station.App_Token, Station.User_Key):
                                                eos_log.info("Voltage alarm sent :" + str(AOS.v_battery) + " volts")
                            else:
                                a.append(stmt.delete(Station.Board))
                                a.append("DELETE from BOARD")
                                eos_log.debug("Will clear BOARD")
                            if Station.Soil_Count == 0:
                                a.append(stmt.delete(Station.Soil))
                                a.append("DELETE from SOIL")
                                eos_log.debug("Will clear SOIL")
                            if len(a) > 0:
                                if eoss.sqlmupdate(db,a) == False:
                                    eos_log.error("Failed to delete BASE records")
                                else:
                                    eos_log.debug("Deleted BASE records")
                                
                        except Exception,e:
                            eos_log.error('On removing data : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On removing data: " + str(e), Station.App_Token, Station.User_Key)                            

## Now do Relay Triggers - is broke until It is fixed by developer
                        ##eosr.sendrelays(db, AOS, eos_log)                      
                        try:                                

#===============================================================================
#                     BUILD DAILY SUMMARY
#                      
#===============================================================================
                            ##remove pervious entry if this is for same day-- 
                            ##on role over it will keep last entry on previous day                       
                            ##WE_DATE, TEMP_AVG, TEMP_HI, TEMP_LOW, HUM_AVG, DEW_AVG, WIND_AVG, WIND_RUN, WIND_HI, WIND_CHILL, HEAT_OUT, THW, THWS, BAR, BAR_LOW,
                            ##BAR_HI, RAIN, SOLAR_RAD_HI, SOLAR_UV, SOLAR_ENERGY, HEAT_DD, COOL_DD FROM D_SUMMARY
                            eos_log.info("Starting Daily Archive")
                            cur.execute("Delete from CORE_DATE where WE_DATE = '" + AOS.we_date + "'")
                            db.commit()
                            cur.execute("Delete from CORE_TIME where WE_Date = '" + AOS.we_date + "'")
                            db.commit()
                            cur.execute("INSERT INTO CORE_TIME (WE_Date) VALUES('" + AOS.we_date + "')")
                            db.commit()
                            cur.execute("SELECT * FROM D_SUMMARY")
                            aa= []
                            for row in cur.fetchall():
                                if row is not None:
                                    aa.append(stmt.core_date(AOS, row))
                                    if Station.Wind_Count > 0:
                                        cur.execute("SELECT COUNT(WIND_DIR), WIND_DIR FROM CORE_DATA WHERE WE_DATE = '" + AOS.we_date + "' GROUP BY WIND_DIR ORDER BY COUNT(WIND_DIR) DESC LIMIT 0,1")
                                        for row in cur.fetchall():
                                            di = row["WIND_DIR"]
                                        aa.append("Update CORE_DATE Set WIND_DIR = '" + di + "' where WE_DATE = '" + AOS.we_date + "'")
                                        cur.execute("Select WE_TIME as WIND_TIME from CORE_DATA where WE_DATE = '" + AOS.we_date + "' order by cast(WIND_HI as DECIMAL(10,2)) desc limit 0,1")
                                        for row in cur.fetchall():
                                            maxtime = str(row["WIND_TIME"])
                                        aa.append("Update CORE_TIME set MAX_WIND = '" + maxtime + "' where WE_DATE = '" + AOS.we_date + "'")
                                    if Station.Temp_Count > 0:
                                        cur.execute("Select WE_TIME as MAX_TIME from CORE_DATA where WE_DATE = '" + AOS.we_date + "' order by cast(TEMP_OUT as DECIMAL(10,2)) desc limit 0,1")
                                        for row in cur.fetchall():
                                            maxtime = str(row["MAX_TIME"])
                                        aa.append("Update CORE_TIME set MAX_TEMP = '" + maxtime + "' where WE_DATE = '" + AOS.we_date + "'")
                                        cur.execute("Select WE_TIME as MIN_TIME from CORE_DATA where WE_DATE = '" + AOS.we_date + "' order by cast(TEMP_OUT as DECIMAL(10,2)) limit 0,1")
                                        for row in cur.fetchall():
                                            mintime = str(row["MIN_TIME"])
                                        aa.append("Update CORE_TIME set MIN_TEMP = '" + mintime + "' where WE_DATE = '" + AOS.we_date + "'")
                                    if Station.Pressure_Count > 0:
                                        cur.execute("Select WE_TIME as MAX_TIME from CORE_DATA where WE_DATE = '" + AOS.we_date + "' order by cast(BAR as DECIMAL(10,2)) desc limit 0,1")
                                        for row in cur.fetchall():
                                            maxtime = str(row["MAX_TIME"])
                                        aa.append("Update CORE_TIME set MAX_BAR = '" + maxtime + "' where WE_DATE = '" + AOS.we_date + "'")
                                        cur.execute("Select WE_TIME as MIN_TIME from CORE_DATA where WE_DATE = '" + AOS.we_date + "' order by cast(BAR as DECIMAL(10,2)) limit 0,1")
                                        for row in cur.fetchall():
                                            mintime = str(row["MIN_TIME"])
                                        aa.append("Update CORE_TIME set MIN_BAR = '" + mintime + "' where WE_DATE = '" + AOS.we_date + "'")
                                    if Station.Solar_Count > 0:
                                        cur.execute("Select COUNT(SOLAR_RAD) as RC from CORE_DATA where WE_DATE = '" + AOS.we_date + "' and SOLAR_RAD > 100 and CLOUDY < " + str(float(Station.Sun_Trigger)))
                                        for row in cur.fetchall():
                                            suncount = float(row["RC"])
                                            sunhr = round(suncount * t.minute / 60,2)
                                        aa.append("Update CORE_DATE Set SUN_HRS = '" \
                                            + str(sunhr) + "' where WE_DATE = '" + AOS.we_date + "'")
                                    
#===============================================================================
#                    INSERT DAILY SUMMARY
#                      REBOOT if internet not working after 1 hour 6 X 10 min intervals
#===============================================================================
                            if len(aa) > 0:
                                if eoss.sqlmupdate(db,aa) == False:
                                    eos_log.error("Failed to Insert Day records")
                                else:
                                    eos_log.debug('Inserted Daily Table Entries')
                            eos_log.info('Inserted All Archive Data Successful')
                            ##See if we want to try a reboot to see if this will fix internet connection at 6 fails
                            cur.execute('Select count(MSGTYPE) as RC from MSGLOG where MSGTYPE = 1')
                            for row in cur.fetchall():
                                if row is not None:
                                    rt = row["RC"]
                                    if rt == 6:
                                        eos_log.error("6 failed attempts, will reboot")
                                        has_db = False
                                        time.sleep(10)
                                        os.system('sudo shutdown -r now')
                                        
  
                        except Exception,e:
                            eos_log.error('Inserting Archive Daily data : ' + str(e))
                            eosp.sendalert(db, 22, Station.date_time, .25, 0, "On Archive Daily data: " + str(e), Station.App_Token, Station.User_Key)

#===============================================================================
#                     COMPLETE
#                     End of Cycle 
#===============================================================================
            ## Stop processing so that reader can populate data              
            eos_log.debug("Finished Cycle. Sleep for " + str(Station.WaitTime))

            time.sleep(Station.WaitTime)

    except (KeyboardInterrupt, SystemExit): #when you press ctrl+c
        print "Exiting Thread..."
        eos_log.info("Exiting via interupt")

#===============================================================================
#                     EXITING for some reason
#                      exit Broker
#===============================================================================
    if BROKER.Address <> '':
        mqttc.loop_stop()
        mqttc.disconnect()

    if has_db:
        eosp.sendalert(db, 23, Station.date_time, .25, 1, "EOS Server Stopped", Station.App_Token, Station.User_Key)
        db.close()
    else:
        if len(Station.User_Key) > 0:
            eosp.sendpushover(Station.App_Token, Station.User_Key, "EOS Server Stopped Unexpectally - No Db", 1)
        eos_log.critical("EOS Server STOPPED - NO DB")
    print "Finished"
    eos_log.info('EOS Server Stopped')

if __name__ ==  '__main__':

    main()
