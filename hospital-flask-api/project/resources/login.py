from flask import Blueprint, request
from ..model import db, Login
from ..config import SECRET_KEY
import bcrypt
import jwt
from datetime import datetime, timedelta


blp = Blueprint('login', __name__)

@blp.route("/login", methods=['POST'])
def handling_login():
    auth = request.get_json()
    username, password = auth['username'], auth['password']
    login = Login.query.filter_by(username=username).first()

    if login == None:
        return {"message": "Failed. Username is not registered."}
    
    if bcrypt.checkpw(password.encode('utf-8'), login.password.encode('utf-8')):
        iat = datetime.utcnow() + timedelta(hours=7)  # UTC+7
        exp = iat + timedelta(minutes=30)

        payload = {
            "sub": f"{username}",
            "iat": int(iat.timestamp()),
            "exp": int(exp.timestamp()),
            "login_id": login.id,
        }
        token = jwt.encode(payload, SECRET_KEY)
        
        return {
            "message": "Success. Username and password are registered.",
            "token": token
        }
    return {"message": "Failed. Password does not match the username."}