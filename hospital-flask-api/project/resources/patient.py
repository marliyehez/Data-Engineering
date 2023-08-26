from flask import Blueprint, request
from ..model import db, Patient
from ..auth import employee_required
from sqlalchemy.exc import IntegrityError
from datetime import datetime


blp = Blueprint('patient', __name__)

@blp.route("/patients", methods=['GET','POST'])
@employee_required
def handling_patients():
    if request.method == 'GET':
        patients = []
        patients_data = Patient.query.all()
        for patient_data in patients_data:
            patient = {
                'id': patient_data.id,
                'name': patient_data.name,
                'gender': patient_data.gender,
                'birthdate': patient_data.birthdate.isoformat(),
                'no_ktp': patient_data.no_ktp,
                'address': patient_data.address,
                'vaccine_type': patient_data.vaccine_type,
                'vaccine_count': patient_data.vaccine_count,
            }
            patients.append(patient)
        return {"patients": patients}

    elif request.method == 'POST':
        patient_data = request.get_json()
        patient_data['birthdate'] = datetime.fromisoformat(patient_data['birthdate']).date()
        try:
            patient = Patient(**patient_data, vaccine_type=None, vaccine_count=None)
            db.session.add(patient)
            db.session.commit()
            patient = {
                    'id': patient.id,
                    'name': patient.name,
                    'gender': patient.gender,
                    'birthdate': patient.birthdate.isoformat(),
                    'no_ktp': patient.no_ktp,
                    'address': patient.address,
                }
            return {"patient": patient, "message": "Success. ID card number is not registered yet."}
        except IntegrityError:
            return {"message": "Failed. ID card number has already been registered."}


@blp.route("/patients/<int:patient_id>", methods=['GET','PUT', 'DELETE'])
@employee_required
def handling_a_patient(patient_id):
    if request.method == 'GET':
        patient = Patient.query.filter_by(id=patient_id).first() 

    elif request.method == 'PUT':
        patient_data = request.get_json()
        patient_data['birthdate'] = datetime.fromisoformat(patient_data['birthdate']).date()
        patient = Patient.query.filter_by(id=patient_id).update(patient_data)
        db.session.commit()
        patient = Patient.query.filter_by(id=patient_id).first()

    elif request.method == 'DELETE':
        patient = Patient.query.filter_by(id=patient_id).first()
        db.session.delete(patient)
        db.session.commit()

    patient = {
        'id': patient.id,
        'name': patient.name,
        'gender': patient.gender,
        'birthdate': patient.birthdate.isoformat(),
        'no_ktp': patient.no_ktp,
        'address': patient.address,
        'vaccine_type': patient.vaccine_type,
        'vaccine_count': patient.vaccine_count,
        'medical_history': [
            {
                'appointment_id': appointment.id,
                'diagnose': appointment.diagnose,
                'notes': appointment.notes,
            }
            for appointment in patient.appointments
            ]
        }
    return {"patient": patient}