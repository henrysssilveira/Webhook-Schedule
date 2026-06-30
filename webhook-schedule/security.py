from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic()

def verify_user(credentials: HTTPBasicCredentials = Depends(security)):
    if (
        credentials.username != "admin"
        or credentials.password != "123456"
    ):
        raise HTTPException(status_code=401)

    return credentials.username