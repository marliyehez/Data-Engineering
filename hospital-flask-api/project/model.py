from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError
from enum import Enum
import bcrypt
from datetime import datetime

db = SQLAlchemy()

class StatusEnum(Enum):
    IN_QUEUE = "IN_QUEUE"
    DONE = "DONE"
    CANCELLED = "CANCELLED"


class Login(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(20), unique=True)
    password = db.Column(db.String(255), nullable=False)


class Employee(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50))
    gender = db.Column(db.String(1))
    birthdate = db.Column(db.Date)
    login_id = db.Column(db.Integer)

    def __repr__(self):
        return f'<Employee: {self.id} {self.name}>'


class Doctor(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50))
    gender = db.Column(db.String(1))
    birthdate = db.Column(db.Date)
    work_start_time = db.Column(db.Time)
    work_end_time = db.Column(db.Time)
    appointments = db.relationship('Appointment', backref='Doctor', lazy=True, cascade="all, delete")
    login_id = db.Column(db.Integer)

    def __repr__(self):
        return f'<Doctor: {self.id} {self.name}>'


class Patient(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50))
    gender = db.Column(db.String(1))
    birthdate = db.Column(db.Date)
    no_ktp = db.Column(db.String(16), unique=True)
    address = db.Column(db.String(50))
    vaccine_type = db.Column(db.String(20))
    vaccine_count = db.Column(db.Integer)
    appointments = db.relationship('Appointment', backref='Patient', lazy=True, cascade="all, delete")
    
    def __repr__(self):
        return f'<Patient: {self.id} {self.name}>'


class Appointment (db.Model):
    id = db.Column(db.Integer, primary_key=True)
    patient_id = db.Column(db.Integer, db.ForeignKey('patient.id'))
    doctor_id = db.Column(db.Integer, db.ForeignKey('doctor.id'))
    datetm = db.Column(db.DateTime)
    status = db.Column(db.Enum(StatusEnum))
    diagnose = db.Column(db.String(90))
    notes = db.Column(db.String(90))

    def __repr__(self):
        return f'<Appointment: {self.id}>'


def initialize_data():
    try :
        # Add admin
        admin_data = {"name": "Admin", "username": "admin", "gender": "M", "password": "pass", "birthdate": "2023-06-30"}
        admin_data['birthdate'] = datetime.fromisoformat(admin_data['birthdate']).date()
        admin_data['password'] = bcrypt.hashpw(admin_data['password'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        login = Login(username=admin_data['username'], password=admin_data['password'])
        db.session.add(login)
        db.session.commit()
        del admin_data["username"]
        del admin_data["password"]
        admin = Employee(**admin_data, login_id=login.id)
        db.session.add(admin)
        db.session.commit()

        # Add first doctor
        doctor_data = {"name": "Doctor 1", "username": "doc1", "password": "pw1", "gender": "F", "birthdate": "2000-06-30", "work_start_time":"08:30:00", "work_end_time":"19:30:00"}
        doctor_data['password'] = bcrypt.hashpw(doctor_data['password'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        doctor_data['birthdate'] = datetime.fromisoformat(doctor_data['birthdate']).date()
        doctor_data['work_start_time'] = datetime.strptime(doctor_data['work_start_time'], "%H:%M:%S").time()
        doctor_data['work_end_time'] = datetime.strptime(doctor_data['work_end_time'], "%H:%M:%S").time()
        login = Login(username=doctor_data['username'], password=doctor_data['password'])
        db.session.add(login)
        db.session.commit()
        del doctor_data["username"]
        del doctor_data["password"]
        doctor = Doctor(**doctor_data, login_id=login.id)
        db.session.add(doctor)
        db.session.commit()
    except IntegrityError:
        pass