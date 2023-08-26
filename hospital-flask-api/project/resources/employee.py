from flask import Blueprint, request
from ..model import db, Login, Employee
from ..auth import employee_required
from sqlalchemy.exc import IntegrityError
from datetime import datetime
import bcrypt


blp = Blueprint('employee', __name__)

@blp.route("/employees", methods=['GET','POST'])
@employee_required
def handling_employees():
    if request.method == 'GET':
        employees = []
        employees_data = Employee.query.all()
        for employee_data in employees_data:
            login_data = Login.query.filter_by(id=employee_data.login_id).first()
            employee = {
                'id': employee_data.id,
                'name': employee_data.name,
                'username': login_data.username,
                'gender': employee_data.gender,
                'birthdate': employee_data.birthdate.isoformat(),
            }
            employees.append(employee)
        return {"employees": employees}

    elif request.method == 'POST':

        # Get and transform the data
        employee_data = request.get_json()
        employee_data['birthdate'] = datetime.fromisoformat(employee_data['birthdate']).date()
        employee_data['password'] = bcrypt.hashpw(employee_data['password'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

        try:
            # Insert login data
            login = Login(username=employee_data['username'], password=employee_data['password'])
            db.session.add(login)
            db.session.commit()
            del employee_data["username"]
            del employee_data["password"]

            # Insert employee data
            employee = Employee(**employee_data, login_id=login.id)
            db.session.add(employee)
            db.session.commit()

            # Return the employee data
            employee = {
                'id': employee.id,
                'name': employee.name,
                'username': login.username,
                'gender': employee.gender,
                'birthdate': employee.birthdate.isoformat(),
            }
            return {"employee": employee, "message": "Success. Username is not registered yet."}
        
        except IntegrityError:
            return {"message": "Failed. Username has already been registered."}



@blp.route("/employees/<int:employee_id>", methods=['GET','PUT','DELETE'])
@employee_required
def handling_an_employee(employee_id):
    if request.method == 'GET':
        employee = Employee.query.filter_by(id=employee_id).first()
        login = Login.query.filter_by(id=employee.login_id).first()

    elif request.method == 'PUT':

        # Get and transform the data
        employee_data = request.get_json()
        employee_data['birthdate'] = datetime.fromisoformat(employee_data['birthdate']).date()
        employee_data['password'] = bcrypt.hashpw(employee_data['password'].encode('utf-8'), bcrypt.gensalt())
        employee = Employee.query.filter_by(id=employee_id).first()

        # Update the login data
        selected_keys = ["username", "password"]
        login_data = {key: employee_data[key] for key in selected_keys if key in employee_data.keys()}
        login = Login.query.filter_by(id=employee.login_id).update(login_data)
        db.session.commit()

        # Update the employee data
        selected_keys = ["name", "gender", "birthdate"]
        employee_data = {key: employee_data[key] for key in selected_keys if key in employee_data.keys()}
        employee = Employee.query.filter_by(id=employee_id).update(employee_data)
        db.session.commit()

        # Retrieve the data back
        employee = Employee.query.filter_by(id=employee_id).first()
        login = Login.query.filter_by(id=employee.login_id).first()
        

    elif request.method == 'DELETE':
        employee = Employee.query.filter_by(id=employee_id).first()
        login = Login.query.filter_by(id=employee.login_id).first()
        db.session.delete(employee)
        db.session.delete(login)
        db.session.commit()

    employee = {
        'id': employee.id,
        'name': employee.name,
        'username': login.username,
        'gender': employee.gender,
        'birthdate': employee.birthdate.isoformat(),
    }
    return {"employee": employee}