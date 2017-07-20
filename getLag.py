#!/usr/bin/env python
import os
import ConfigParser
import smtplib
import sqlite3
import pykafka
from pykafka.exceptions import KafkaException
from kazoo.exceptions import ZookeeperError
from threading import Thread
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from kazoo.client import KazooClient


def getOffsetSize(zk, consumer, topic, partition, zkOffsets):
    zkOffsets.append(int(zk.get("/consumers/%s/offsets/%s/%s" % (consumer, topic, partition))[0]))


def lagCalculator(zk, consumer, topic, client):
    if topic not in client.topics:
        errors[app] = 'Topic {} does not exist.'.format(topic)
        return
    topicObj = client.topics[topic]
    latest_offsets = topicObj.latest_available_offsets()
    topicSize = sum(i[0][0] for i in latest_offsets.values())

    zkOffsets = []
    if zk.exists("/consumers/%s/offsets/%s" % (consumer, topic)):
        partitions = zk.get_children("/consumers/%s/offsets/%s" % (consumer, topic))
        threads = [None] * len(partitions)
        i = 0
        for p in partitions:
            threads[i] = Thread(target=getOffsetSize, args=(zk, consumer, topic, p, zkOffsets))
            threads[i].start()
            i += 1
        for i in range(len(threads)):
            threads[i].join()
    else:
        errors[app] = 'Topic {} does not exist.'.format(topic)
    zkOffsetsConsumed = sum(zkOffsets)
    lag = topicSize - zkOffsetsConsumed
    print datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\t" + app + " " + topic + "\t" + str(lag)
    threadConn = sqlite3.connect(databaseFile, detect_types=sqlite3.PARSE_DECLTYPES)
    threadConn.execute('''insert INTO lag VALUES(?,?,?,?,?)''', (timeStampObj, app, consumer, topic, lag))
    threadConn.commit()
    threadConn.close()


def sendMail(message):
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = recipient[0]
    s = smtplib.SMTP(smtp)
    s.sendmail(sender, recipient, msg.as_string())
    s.quit()

timeStampObj = datetime.now()
timeStamp = timeStampObj.strftime('%Y-%m-%d %H:%M:%S')
# Get current folder
workDir = (os.path.dirname(os.path.realpath(__file__)))
configFile = os.path.join(workDir, "kfkmgr.ini")
databaseFile = os.path.join(workDir, "lagsDB.sqlite3")
# Read config file
cp = ConfigParser.ConfigParser()
cp.read(configFile)
generalSection = "general"
threshold = int(cp.get(generalSection, "threshold"))
checkInterval = int(cp.get(generalSection, "checkInterval"))
minNumberOfValues = int(cp.get(generalSection, "minNumberOfValues"))
timeToCheck = int(cp.get(generalSection, "timeToCheck"))
sender = cp.get(generalSection, "sender")
recipient = [x.strip() for x in cp.get(generalSection, "recipient").split(',')]
smtp = cp.get(generalSection, "smtp")
subject = cp.get(generalSection, "subject")

# Connect to sqlite DB
conn = sqlite3.connect(databaseFile, detect_types=sqlite3.PARSE_DECLTYPES)
cursor = conn.cursor()
# Check if table exist in DB
if len(cursor.execute('''SELECT name FROM sqlite_master WHERE type='table' and name='lag' ''').fetchall()) != 1:
    cursor.execute('''CREATE TABLE lag
                        (timestamp timestamp, application text, consumer text, topic text, lag integer)''')
    cursor.execute('''INSERT INTO lag VALUES(?,?,?,?,?)''', (timeStampObj, 'timestamp', None, None, None))
    conn.commit()
errors = {}
response = None
lag = ""

# Go through each section in config file (except general section)
for app in cp.sections():
    if app != generalSection:
        # Read config to get application specific values
        broker = cp.get(app, "broker")
        zookeeper = cp.get(app, "zookeeper")
        consumer = cp.get(app, "consumer")

        # Connect to kafka broker
        try:
            client = pykafka.KafkaClient(hosts=broker)
        except KafkaException:
            errors[app] = 'Can not connect to kafka broker {}'.format(broker)
            print errors[app]
            continue

        # Connect to zookeeper
        try:
            zk = KazooClient(hosts=zookeeper, read_only=True)
            zk.start()
        except ZookeeperError:
            errors[app] = 'Can not connect to zookeeper {}'.format(broker)
            print errors[app]
            continue
        # Get topic list for consumer
        topics = []
        if zk.exists("/consumers/%s" % consumer):
            topics = zk.get_children("/consumers/%s/offsets" % consumer)
        else:
            errors[app] = 'Consumer {} does not exist.'.format(consumer)
            continue

        # Start new thread for each topic current consumer is subscribed to
        j = 0
        threadsJ = [None] * len(topics)
        for topic in topics:
            topic = str(topic)
            threadsJ[j] = Thread(target=lagCalculator, args=(zk, consumer, topic, client))
            threadsJ[j].start()
            j += 1
        for i in range(len(threadsJ)):
                threadsJ[i].join()
        zk.stop()

dbTimeStamp = conn.execute('''select timestamp from lag where application = 'timestamp' ''').fetchone()[0]
checkTime = timeStampObj - timedelta(minutes=checkInterval)
timeToCheckDelta = timeStampObj - timedelta(minutes=timeToCheck)
if dbTimeStamp < checkTime:
    print timeStamp + "\tEvaluating values\t" + dbTimeStamp.strftime('%Y-%m-%d %H:%M:%S')
    for app in cp.sections():
        if app != generalSection:
            for topicName in conn.execute('''select distinct topic from lag where application = ? and timestamp > ? ''',
                                          (app, timeToCheckDelta)).fetchall():
                topicName = str(topicName[0])
                lags = conn.execute('''select lag from lag where application = ? and topic = ? and timestamp > ? 
                                        order by timestamp''',
                                    (app, topicName, timeToCheckDelta)).fetchall()
                comparisonResult = []
                lags = [x[0] for x in lags]
                if len(lags) >= minNumberOfValues:
                    for i in range(minNumberOfValues):
                        if lags[-i] >= lags[-i-1] >= threshold:
                            comparisonResult.append(True)
                        else:
                            comparisonResult.append(False)
                    if False not in comparisonResult:
                        if app in errors:
                            errors[app] += "\n" + topicName + "\t" + str(lags)
                        else:
                            errors[app] = topicName + "\t" + str(lags)
                else:
                    print "%s Lack of values. Values number: %s. Minimum required: %s" % (app, len(lags), minNumberOfValues)
    conn.execute('''update lag set timestamp = ? where application = ? ''', (timeStampObj, 'timestamp'))
    if len(errors) > 0:
        message = ""
        for error in errors.keys():
            message += timeStamp + "\t" + error + "\t" + errors[error] + "\n"
        sendMail(message)
conn.commit()
conn.close()
