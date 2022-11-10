#!/usr/bin/python3

# This script will read data from serial connected to the digital meter P1 port

# Created by Jens Depuydt
# https://www.jensd.be
# https://github.com/jensdepuydt

import serial
import sys
import crcmod.predefined
import re
from tabulate import tabulate
import mysql.connector
import sqlite3
from datetime import datetime
from datetime import timezone
import time

# Change your serial port here:
serialport = '/dev/ttyUSB0'

# Enable debug if needed:
debug = True

# MySQL Host
host = "127.0.0.1"
conn = None
curs = None

# Add/update OBIS codes here:
# obiscodes = {
#     "0-0:1.0.0": "Timestamp",
#     "0-0:96.3.10": "Switch electricity",
#     "0-1:24.4.0": "Switch gas",
#     "0-0:96.1.1": "Meter serial electricity",
#     "0-1:96.1.1": "Meter serial gas",
#     "0-0:96.14.0": "Current rate (1=day,2=night)",
#     "1-0:1.8.1": "Rate 1 (day) - total consumption",
#     "1-0:1.8.2": "Rate 2 (night) - total consumption",
#     "1-0:2.8.1": "Rate 1 (day) - total production",
#     "1-0:2.8.2": "Rate 2 (night) - total production",
#     "1-0:21.7.0": "L1 consumption",
#     "1-0:41.7.0": "L2 consumption",
#     "1-0:61.7.0": "L3 consumption",
#     "1-0:1.7.0": "All phases consumption",
#     "1-0:22.7.0": "L1 production",
#     "1-0:42.7.0": "L2 production",
#     "1-0:62.7.0": "L3 production",
#     "1-0:2.7.0": "All phases production",
#     "1-0:32.7.0": "L1 voltage",
#     "1-0:52.7.0": "L2 voltage",
#     "1-0:72.7.0": "L3 voltage",
#     "1-0:31.7.0": "L1 current",
#     "1-0:51.7.0": "L2 current",
#     "1-0:71.7.0": "L3 current",
#     "0-1:24.2.3": "Gas consumption"
#     }

obiscodes = {
    "0-0:1.0.0":"Datum och tid",
    "1-0:1.8.0":"Mätarställning Aktiv Energi Uttag",
    "1-0:2.8.0":"Mätarställning Aktiv Energi Inmatning",
    "1-0:3.8.0":"Mätarställning Reaktiv Energi Uttag",
    "1-0:4.8.0":"Mätarställning Reaktiv Energi Inmatning",
    "1-0:1.7.0":"Aktiv Effekt Uttag	Momentan trefaseffekt",
    "1-0:2.7.0":"Aktiv Effekt Inmatning	Momentan trefaseffekt",
    "1-0:3.7.0":"Reaktiv Effekt Uttag	Momentan trefaseffekt",
    "1-0:4.7.0":"Reaktiv Effekt Inmatning	Momentan trefaseffekt",
    "1-0:21.7.0":"L1 Aktiv Effekt Uttag	Momentan effekt",
    "1-0:22.7.0":"L1 Aktiv Effekt Inmatning	Momentan effekt",
    "1-0:41.7.0":"L2 Aktiv Effekt Uttag	Momentan effekt",
    "1-0:42.7.0":"L2 Aktiv Effekt Inmatning	Momentan effekt",
    "1-0:61.7.0":"L3 Aktiv Effekt Uttag	Momentan effekt",
    "1-0:62.7.0":"L3 Aktiv Effekt Inmatning	Momentan effekt",
    "1-0:23.7.0":"L1 Reaktiv Effekt Uttag	Momentan effekt",
    "1-0:24.7.0":"L1 Reaktiv Effekt Inmatning	Momentan effekt",
    "1-0:43.7.0":"L2 Reaktiv Effekt Uttag	Momentan effekt",
    "1-0:44.7.0":"L2 Reaktiv Effekt Inmatning	Momentan effekt",
    "1-0:63.7.0":"L3 Reaktiv Effekt Uttag	Momentan effekt",
    "1-0:64.7.0":"L3 Reaktiv Effekt Inmatning	Momentan effekt",
    "1-0:32.7.0":"L1 Fasspänning	Momentant RMS-värde",
    "1-0:52.7.0":"L2 Fasspänning	Momentant RMS-värde",
    "1-0:72.7.0":"L3 Fasspänning	Momentant RMS-värde",
    "1-0:31.7.0":"L1 Fasström	Momentant RMS-värde",
    "1-0:51.7.0":"L2 Fasström	Momentant RMS-värde",
    "1-0:71.7.0":"L3 Fasström	Momentant RMS-värde"
    }

def checkcrc(p1telegram):
    # check CRC16 checksum of telegram and return False if not matching
    # split telegram in contents and CRC16 checksum (format:contents!crc)
    for match in re.compile(b'\r\n(?=!)').finditer(p1telegram):
        p1contents = p1telegram[:match.end() + 1]
        # CRC is in hex, so we need to make sure the format is correct
        givencrc = hex(int(p1telegram[match.end() + 1:].decode('ascii').strip(), 16))
    # calculate checksum of the contents
    calccrc = hex(crcmod.predefined.mkPredefinedCrcFun('crc16')(p1contents))
    # check if given and calculated match
    if debug:
        print(f"Given checksum: {givencrc}, Calculated checksum: {calccrc}")
    if givencrc != calccrc:
        if debug:
            print("Checksum incorrect, skipping...")
        return False
    return True


def parsetelegramline(p1line):
    # parse a single line of the telegram and try to get relevant data from it
    unit = ""
    timestamp = ""
    if debug:
        print(f"Parsing:{p1line}")
    # get OBIS code from line (format:OBIS(value)
    obis = p1line.split("(")[0]
    if debug:
        print(f"OBIS:{obis}")
    # check if OBIS code is something we know and parse it
    if obis in obiscodes:
        # get values from line.
        # format:OBIS(value), gas: OBIS(timestamp)(value)
        values = re.findall(r'\(.*?\)', p1line)
        value = values[0][1:-1]
        # timestamp requires removal of last char
        if obis == "0-0:1.0.0" or len(values) > 1:
            value = value[:-1]
        # report of connected gas-meter...
        if len(values) > 1:
            timestamp = value
            value = values[1][1:-1]
        # serial numbers need different parsing: (hex to ascii)
        if "96.1.1" in obis:
            value = bytearray.fromhex(value).decode()
        else:
            # separate value and unit (format:value*unit)
            lvalue = value.split("*")
            value = float(lvalue[0])
            if len(lvalue) > 1:
                unit = lvalue[1]
        # return result in tuple: description,value,unit,timestamp
        if debug:
            print (f"description:{obiscodes[obis]}, \
                     value:{value}, \
                     unit:{unit}")
        return (obiscodes[obis], value, unit)
    else:
        return ()

# Upload data to server
def uploadData(timeStamp,rate):
    try:
        mydb = mysql.connector.connect(
            host=host,
            user="energy",
            password="UKJkXjpBmgjeYhRZ",
            database="Energy")
        mycursor = mydb.cursor()
        
        sql = "INSERT INTO Log (Logtime, Rate) VALUES (%s,%s)" % (timeStamp, rate)
        mycursor.execute(sql)
        mydb.commit()

        if debug:
            print("pulses uploaded.", flush=True)

    except mysql.connector.Error as e:
        print(" - Error %s [%s]" % (e, str(datetime.now())), flush=True)
        print (" - Server Offline [%s]" % str(datetime.now()), flush=True)


def main():
    ser = serial.Serial(serialport, 115200, xonxoff=1)
    p1telegram = bytearray()
    while True:
        try:
            # read input from serial port
            p1line = ser.readline()
            timeStamp = None
            rate = None

            if debug:
                print ("Reading: ", p1line.strip())
            # P1 telegram starts with /
            # We need to create a new empty telegram
            if "/" in p1line.decode('ascii'):
                if debug:
                    print ("Found beginning of P1 telegram")
                p1telegram = bytearray()
                print('*' * 60 + "\n")
            # add line to complete telegram
            p1telegram.extend(p1line)
            # P1 telegram ends with ! + CRC16 checksum
            if "!" in p1line.decode('ascii'):
                if debug:
                    print("Found end, printing full telegram")
                    print('*' * 40)
                    print(p1telegram.decode('ascii').strip())
                    print('*' * 40)
                if checkcrc(p1telegram):
                    # parse telegram contents, line by line
                    output = []
                    for line in p1telegram.split(b'\r\n'):
                        r = parsetelegramline(line.decode('ascii'))
                        if r:
                            if r[0] == "Datum och tid":
                                timeStamp = r[1]
                            if r[0] == "Aktiv Effekt Uttag	Momentan trefaseffekt":
                                rate = r[1]    
                            if timeStamp != None and rate != None:
                                print("upload")
                                convertedTime = time.strptime(str(int(timeStamp)), '%Y%m%d%H%M%S')
                                timestamp = convertedTime.replace(tzinfo=timezone.utc).timestamp()
                                uploadData(timeStamp=timeStamp, rate=rate)
                                rate = None
                                timeStamp = None
                            output.append(r)
                            if debug:
                                print(f"desc:{r[0]}, val:{r[1]}, u:{r[2]}")
                    print(tabulate(output,
                        headers=['Description', 'Value', 'Unit'],
                        tablefmt='github'))
        except KeyboardInterrupt:
            print("Stopping...")
            ser.close()
            break
        except:
            # if debug:
            #     print(traceback.format_exc())
            # print(traceback.format_exc())
            print ("Something went wrong...")
            
            # flush the buffer
            ser.flush()
            
            ser.close()


if __name__ == '__main__':
    main()
