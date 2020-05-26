import pika
from flask import Flask,request, jsonify,render_template,abort, session,Response
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import json
import os
import re
import requests
import sys
from sqlalchemy.sql import select,exists
from sqlalchemy.orm import load_only
import time
from sqlalchemy.orm import backref

app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'rideshare.db')
db = SQLAlchemy(app)
ma = Marshmallow(app)



class User(db.Model):

    __tablename__ = "user"

    user_id = db.Column(db.Integer, primary_key=True, autoincrement=True, nullable=False)
    username = db.Column(db.String(80), unique=True)
    password = db.Column(db.String(128))
    posts = db.relationship('UserRide', backref='user', lazy='dynamic')
    ride = db.relationship('Ride', cascade='all,delete',secondary= 'user_ride', backref='user')

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def serialize(self):
        return {
            'user_id': self.user_id,
            'username': self.username,
            'password':self.password}


class Ride(db.Model):

    __tablename__ = "ride"

    ride_id = db.Column(db.Integer,autoincrement=True,primary_key=True)
    created= db.Column(db.String(50), nullable=False)
    src_adr= db.Column(db.String(50), nullable=False)
    dest_adr = db.Column(db.String(50), nullable=False)
    timestamp = db.Column(db.String(50))


    def serialize(self):
        return {
        'ride_id':self.ride_id,
        'created':self.created,
        'src_adr':self.src_adr,
        'dest_adr':self.dest_adr,
        'timestamp':self.timestamp,
        }

class UserRide(db.Model):

    __tablename__ = "user_ride"

    user_ride_id = db.Column(db.Integer,
                          autoincrement=True,
                          primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.user_id"), nullable=False)
    ride_id = db.Column(db.Integer, db.ForeignKey("ride.ride_id"), nullable=False)
    ride = db.relationship('Ride', uselist=False)


    def __repr__(self):
        s = "<UserRide user_ride_id=%s user_id=%s ride_id=%s >"
        return s % (self.user_ride_id, self.user_id, self.ride_id)

class joinRide(db.Model):
    __tablename__ = "userdetails"


    user_ride_id = db.Column(db.Integer,
                          autoincrement=True,
                          primary_key=True)
    username = db.Column(db.String(80), nullable=False)
    ride_id = db.Column(db.Integer, db.ForeignKey("ride.ride_id"), nullable=False)
    ride = db.relationship('Ride', uselist=False)

    def serialize(self):
        return {
        'user_ride_id':self.user_ride_id,
        'username':self.username,
        'ride_id':self.ride_id

        }

class DataDisplay(ma.Schema):
    class Meta:
        fields = ('username','password','user_id','ride_id','created','src_adr','dest_adr','timestamp')


data_display = DataDisplay()
datas_display = DataDisplay(many=True)


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))

channel1 = connection.channel()

channel1.queue_declare(queue='writeQ')
channel1.queue_declare(queue='readQ')

#write function
def writedb(sqlquery):
    if 'read' in sqlquery['query']:
        if "user" in sqlquery['table']:
            if "username" in sqlquery['where']:
            #print("hello")
                if sqlquery['data']:
                #print("hello")
                    uname="{}".format(sqlquery['data'])
                    existing_user = User.query.filter(User.username == uname).first()
                    if existing_user:
                        return 200
    if 'insert' in sqlquery['query']:
        if 'user' in sqlquery['table']:
            nuser="{}".format(sqlquery['username'])
            pwdd="{}".format(sqlquery['password'])
            useradd = User(username=nuser,password=pwdd)
            db.session.add(useradd)
            db.session.commit()
            return 200
        if 'ride' in sqlquery['table']:
                by="{}".format(sqlquery['name'])
                pick="{}".format(sqlquery['source'])
                dest="{}".format(sqlquery['destiny'])
                schedule="{}".format(sqlquery['date'])
                rideadd = Ride(created=by,src_adr=pick,dest_adr=dest,timestamp=schedule)
                db.session.add(rideadd)
                db.session.commit()
                return 200
        if 'details' in sqlquery['table']:
                nuserr="{}".format(sqlquery['usernn'])
                uid="{}".format(sqlquery['rides'])
                new_ride = joinRide(username=nuserr, ride_id=uid)
                db.session.add(new_ride)
                db.session.commit()
                return 201

    if 'delete' in sqlquery['query']:
        if 'user' in sqlquery['table']:
            nuser="{}".format(sqlquery['usern'])
            existing_user = User.query.filter(User.username == nuser).first()
            if existing_user:
                    new2=Ride.query.filter(Ride.created ==  nuser).first()
                    if new2:
                        db.session.delete(new2)
                        db.session.delete(existing_user)
                        db.session.commit()
                        return 200
                    else:
                        db.session.delete(existing_user)
                        db.session.commit()
                        return 200

        if 'ride' in sqlquery['table']:
                rides="{}".format(sqlquery['insert'])
                new2=Ride.query.filter(Ride.ride_id == rides).first()
                db.session.delete(new2)
                db.session.commit()
                return 200

    if 'clear' in sqlquery['query']:
        if 'user' in sqlquery['table']:
            userlist=db.session.execute('select * from user');
            res=datas_display.dump(userlist)
            if res:
                db.session.execute('delete * from user');
                db.session.commit()
                return Response(status=200)
            else:
                return Response(status=204)

        if 'ride' in sqlquery['table']:
            userlist=db.session.execute('select * from ride');
            res=datas_display.dump(userlist)
            if res:
                db.session.execute('delete * from ride');
                db.session.commit()
                return Response(status=200)
            else:
                return Response(status=204)

        if 'userride' in sqlquery['table']:
            userlist=db.session.execute('select * from user_ride');
            res=datas_display.dump(userlist)
            if res:
                db.session.execute('delete * from user_ride');
                db.session.commit()
                return Response(status=200)
            else:
                return Response(status=204)

        if 'userdetails' in sqlquery['table']:
            userlist=db.session.execute('select * from userdetails');
            res=datas_display.dump(userlist)
            if res:
                db.session.execute('delete * from user');
                db.session.commit()
                return Response(status=200)
            else:
                return Response(status=204)


def on_request1(ch, method, props, body):
    response = writedb(json.loads(body))
    #print(" writting to queue success", n)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)


def master():
  channel1.queue_declare(queue='writeQ')
  channel1.basic_consume(queue='writeQ',on_message_callback=on_request1)
  channel1.start_consuming()


#readfunction
def readdb(readquery):
    if "userlist" in readquery['table']:
        sql= db.session.query(User.username)
        if sql:
            result = datas_display.dump(sql)
            l=[]
            for i in result:
            l.append(i["username"])
            return l,200
        else:
            return 204
    if 'ridedetails' in readquery['table']:
        res=db.session.execute('select ride_id,created,timestamp from ride     where src_adr={} and dest_adr={}'.format(readquery['source'],['destiny']))
        if res:
            result = datas_display.dump(res)
            return result,200
        else:
            return 204

    if "user" in readquery['table']:
        if "username" in readquery['where']:
            #print("hello")
            if readquery['data']:
                #print("hello")
                uname="{}".format(readquery['data'])
                existing_user = User.query.filter(User.username == uname).first()
                count=0
                val={}
                val[count]=existing_user
                a={"flag":"empty"}
                if existing_user:
                    return jsonify(val)
                else:
                    return jsonify(a)


    if "ridess" in readquery['table']:
        if "list" in readquery['query']:
            rides="{}".format(readquery['id'])
            existing_user = Ride.query.filter(Ride.ride_id == rides).first()
            if existing_user:
                userlist=db.session.execute('select username from userdetails where ride_id={}'.format(readquery['id']))
                res=datas_display.dump(userlist)
                records = db.session.execute('select ride_id,created,src_adr,dest_adr,timestamp from ride where ride_id={}'.format(readquery['id']))
                for record in records:
                    recordObject = { 'ride_id':record.ride_id,
                    'created_by': record.created,
                            'source': record.src_adr,
                            'users':[],
                            'destination': record.dest_adr,
                            'timestamp': record.timestamp}

                for names in res:
                    recordObject["users"].append(names["username"])
                #recordObject['users'].append(user)

                return recordObject
            else:
                return 400

    if "rideid" in readquery['table']:
            if readquery['id']:
                datas="{}".format(readquery['id'])
                existing_user = Ride.query.filter(Ride.ride_id == datas).first()
                if existing_user:
                    return 200

    if "rides" in readquery['table']:
        rides="{}".format(readquery['id'])
        createdby=db.session.execute('select created from ride where ride_id={}'.format(readquery['id'])).first()
        res=datas_display.dump(createdby)
        return res



def on_request2(ch, method, props, body):
    response = readdb(json.loads(body))

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)


def slave():
  channel1.queue_declare(queue='readQ')
  channel1.basic_consume(queue='readQ',on_message_callback=on_request2)
  channel1.start_consuming()



#print(" [x] Awaiting RPC requests")
#channel.start_consuming()



if(int(sys.argv[1])):
      db.create_all()
      master()
else:
      with app.app_context():
          db.create_all()
          slave()
