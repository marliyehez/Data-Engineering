from flask import Blueprint, request
from ..model import db, Appointment, StatusEnum, Doctor
from ..auth import employee_required
from datetime import datetime


blp = Blueprint('appointment', __name__)

@blp.route("/appointments", methods=['GET','POST'])
@employee_required
def handling_appointments():
    if request.method == 'GET':
        appointments = []
        appointments_data = Appointment.query.all()
        for appointment_data in appointments_data:
            appointment = {
                'id': appointment_data.id,
                'patient_id': appointment_data.patient_id,
                'doctor_id': appointment_data.doctor_id,
                'datetm': appointment_data.datetm,
                'status': appointment_data.status.value,
                'diagnose': appointment_data.diagnose,
                'notes': appointment_data.notes,
            }
            appointments.append(appointment)
        return {"appointments": appointments}

    elif request.method == 'POST':
        
        # Get and transform the data
        appointment_data = request.get_json()
        appointment_data['datetm'] = datetime.fromisoformat(appointment_data['datetm'])
        appointment_data['status'] = StatusEnum(appointment_data['status']) 

        # Time requested
        doctor = Doctor.query.filter_by(id=appointment_data['doctor_id']).first()
        time_requested = appointment_data['datetm']
        
        # Doctor's working hours validation
        if (
            time_requested.time() < doctor.work_start_time 
            or time_requested.time() > doctor.work_end_time
        ):
            return {"message": "Failed. The requested appointment  time is outside the doctor's working hours."}

        # Booked validation
        if (
            time_requested in [
                appointment.datetm for appointment in doctor.appointments 
                                   if appointment.status.value == "IN_QUEUE"
            ]
        ):
            return {"message": "Failed. The requested appointment time has already been booked by another patient."}

        appointment = Appointment(**appointment_data)
        db.session.add(appointment)
        db.session.commit()
        appointment = {
                'id': appointment.id,
                'patient_id': appointment.patient_id,
                'doctor_id': appointment.doctor_id,
                'datetm': appointment.datetm,
                'status': appointment.status.value,
                'diagnose': appointment.diagnose,
                'notes': appointment.notes,
            }
        return {"appointment": appointment, "message": "Success. Your appointment validation process is fulfilled."}


@blp.route("/appointments/<int:appointment_id>", methods=['GET','PUT','DELETE'])
@employee_required
def handling_an_appointment(appointment_id):
    if request.method == 'GET':
        appointment = Appointment.query.filter_by(id=appointment_id).first()

    elif request.method == 'PUT':
        appointment_data = request.get_json()
        appointment_data['datetm'] = datetime.fromisoformat(appointment_data['datetm'])
        appointment = Appointment.query.filter_by(id=appointment_id).update(appointment_data)
        db.session.commit()
        appointment = Appointment.query.filter_by(id=appointment_id).first()

    elif request.method == 'DELETE':
        appointment = Appointment.query.filter_by(id=appointment_id).first()
        db.session.delete(appointment)
        db.session.commit()

    appointment = {
        'id': appointment.id,
        'patient_id': appointment.patient_id,
        'doctor_id': appointment.doctor_id,
        'datetm': appointment.datetm,
        'status': appointment.status.value,
        'diagnose': appointment.diagnose,
        'notes': appointment.notes,
        }
    return {"appointment": appointment}