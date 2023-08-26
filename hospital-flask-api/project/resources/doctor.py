from flask import Blueprint, request
from ..model import db, Login, Doctor
from ..auth import employee_required
from sqlalchemy.exc import IntegrityError
from datetime import datetime
import bcrypt


blp = Blueprint('doctor', __name__)

@blp.route("/doctors", methods=['GET','POST'])
@employee_required
def handling_doctors():
    if request.method == 'GET':
        doctors = []
        doctors_data = Doctor.query.all()
        for doctor_data in doctors_data:
            login_data = Login.query.filter_by(id=doctor_data.login_id).first()
            doctor = {
                'id': doctor_data.id,
                'name': doctor_data.name,
                'username': login_data.username,
                'gender': doctor_data.gender,
                'birthdate': doctor_data.birthdate.isoformat(),
                'work_start_time': doctor_data.work_start_time.strftime("%H:%M:%S"),
                'work_end_time': doctor_data.work_end_time.strftime("%H:%M:%S"),            
            }
            doctors.append(doctor)
        return {"doctors": doctors}

    elif request.method == 'POST':
    
        # Get and transform the data
        doctor_data = request.get_json()
        doctor_data['password'] = bcrypt.hashpw(doctor_data['password'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        doctor_data['birthdate'] = datetime.fromisoformat(doctor_data['birthdate']).date()
        doctor_data['work_start_time'] = datetime.strptime(doctor_data['work_start_time'], "%H:%M:%S").time()
        doctor_data['work_end_time'] = datetime.strptime(doctor_data['work_end_time'], "%H:%M:%S").time()

        try:
            # Insert login data
            login = Login(username=doctor_data['username'], password=doctor_data['password'])
            db.session.add(login)
            db.session.commit()
            del doctor_data["username"]
            del doctor_data["password"]
            
            # Insert doctor data
            doctor = Doctor(**doctor_data, login_id=login.id)
            db.session.add(doctor)
            db.session.commit()

            # Return the doctor data
            doctor = {
                'id': doctor.id,
                'name': doctor.name,
                'username': login.username,
                'gender': doctor.gender,
                'birthdate': doctor.birthdate.isoformat(),
                'work_start_time': doctor.work_start_time.strftime("%H:%M:%S"),
                'work_end_time': doctor.work_end_time.strftime("%H:%M:%S"),
            }
            return {"doctor": doctor, "message": "Success. Username is not registered yet."}
        
        except IntegrityError:
            return {"message": "Failed. Username has already been registered."}


@blp.route("/doctors/<int:doctor_id>", methods=['GET','PUT','DELETE'])
def handling_a_doctor(doctor_id):
    if request.method == 'GET':
        doctor = Doctor.query.filter_by(id=doctor_id).first()
        login = Login.query.filter_by(id=doctor.login_id).first()

    elif request.method == 'PUT':

        # Get and transform the data
        doctor_data = request.get_json()
        doctor_data['birthdate'] = datetime.fromisoformat(doctor_data['birthdate']).date()
        doctor_data['work_start_time'] = datetime.strptime(doctor_data['work_start_time'], "%H:%M:%S").time()
        doctor_data['work_end_time'] = datetime.strptime(doctor_data['work_end_time'], "%H:%M:%S").time()
        doctor = Doctor.query.filter_by(id=doctor_id).first()

        # Update the login data
        selected_keys = ["username", "password"]
        login_data = {key: doctor_data[key] for key in selected_keys if key in doctor_data.keys()}
        login = Login.query.filter_by(id=doctor.login_id).update(login_data)
        db.session.commit()

        # Update the doctor data
        selected_keys = ["name", "gender", "birthdate", "work_start_time", "work_end_time", "appointments", "login_id"]
        doctor_data = {key: doctor_data[key] for key in selected_keys if key in doctor_data.keys()}
        doctor = Doctor.query.filter_by(id=doctor_id).update(doctor_data)
        db.session.commit()

        # Retrieve the data back
        doctor = Doctor.query.filter_by(id=doctor_id).first()
        login = Login.query.filter_by(id=doctor.login_id).first()

    elif request.method == 'DELETE':
        doctor = Doctor.query.filter_by(id=doctor_id).first()
        login = Login.query.filter_by(id=doctor.login_id).first()
        db.session.delete(doctor)
        db.session.delete(login)
        db.session.commit()
    
    doctor = {
        'id': doctor.id,
        'name': doctor.name,
        'username': login.username,
        'gender': doctor.gender,
        'birthdate': doctor.birthdate.isoformat(),
        'work_start_time': doctor.work_start_time.strftime("%H:%M:%S"),
        'work_end_time': doctor.work_end_time.strftime("%H:%M:%S"),
        'appointments_id': [appointment.id for appointment in doctor.appointments],
        }
    return {"doctor": doctor}