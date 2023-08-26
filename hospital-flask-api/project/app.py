from flask import Flask
from google.cloud import bigquery
from .model import db, Patient, initialize_data
from flask_apscheduler import APScheduler

from .resources.appointment import blp as AppointmentBlueprint
from .resources.doctor import blp as DoctorBlueprint
from .resources.employee import blp as EmployeeBlueprint
from .resources.login import blp as LoginBlueprint
from .resources.patient import blp as PatientBlueprint


def create_app(config_pyfile='config.py'):
    app = Flask(__name__)
    app.config.from_pyfile(config_pyfile)

    db.init_app(app)
    with app.app_context():
        db.create_all()
        initialize_data()

    app.register_blueprint(AppointmentBlueprint)
    app.register_blueprint(DoctorBlueprint)
    app.register_blueprint(EmployeeBlueprint)
    app.register_blueprint(LoginBlueprint)
    app.register_blueprint(PatientBlueprint)

    return app


# APP -----------

app = create_app()

def update_vaccine_info():
    with app.app_context():
        patients = Patient.query.all()
        no_ktp_list = [patient.no_ktp for patient in patients]
        no_ktp_list = ', '.join(no_ktp_list)

        if no_ktp_list:
            client = bigquery.Client.from_service_account_json('my-project-390806-8bdde486c5da.json')
            query = f"""
                SELECT CAST(no_ktp AS STRING) AS no_ktp, vaccine_type, vaccine_count
                FROM `my-project-390806.tht.vaccine-data-dummy`
                WHERE no_ktp IN ({no_ktp_list})
                """
            query_job = client.query(query)

            for row in query_job:
                patient_data = dict(row.items())
                patient = Patient.query.filter_by(no_ktp=patient_data['no_ktp']).update(patient_data)
                db.session.commit()
        
            print('Updated!')
        
        else:
            print('No patients yet.')

# Scheduler
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.add_job(id="job1", func=update_vaccine_info, trigger="interval", seconds=15)
scheduler.start()