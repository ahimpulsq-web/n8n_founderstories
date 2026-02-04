from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from n8n_founderstories.services.mailer.runner import (
    start_mailer,
    generate_mailer_content_all,   # <— changed
)

from n8n_founderstories.services.mailer.auth import validate_mailer_auth


router = APIRouter()


class MailerRequest(BaseModel):
    request_id: str = Field(..., min_length=1)
    spreadsheet_id: str = Field(..., min_length=1)
    batch_size: int = Field(2, ge=1, le=200)

class MailerAuthRequest(BaseModel):
    spreadsheet_id: str = Field(..., min_length=1)
    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)



@router.post("/mailer/contentwriter")
def start_mailer_endpoint(payload: MailerRequest):
    request_id = payload.request_id.strip()
    spreadsheet_id = payload.spreadsheet_id.strip()
    batch_size = payload.batch_size

    print(request_id)
    print(spreadsheet_id)
    print(batch_size)

    if not request_id:
        raise HTTPException(status_code=400, detail="request_id is required")
    if not spreadsheet_id:
        raise HTTPException(status_code=400, detail="spreadsheet_id is required")

    start_mailer(request_id)

    generate_mailer_content_all(
        request_id=request_id,
        spreadsheet_id=spreadsheet_id,
        batch_size=batch_size,
    )

    return {
        "request_id": request_id,
        "spreadsheet_id": spreadsheet_id,
        "status": "ok",
    }


@router.post("/mailer/auth")
def mailer_auth_start(payload: MailerAuthRequest):
    spreadsheet_id = payload.spreadsheet_id.strip()

    if not spreadsheet_id:
        raise HTTPException(status_code=400, detail="spreadsheet_id is required")

    if not validate_mailer_auth(payload.username, payload.password):
        raise HTTPException(status_code=403, detail="Invalid mailer credentials")

    # optional: return data n8n needs
    return {
        "status": "ok",
        "authenticated": True,
        "spreadsheet_id": spreadsheet_id,
    }



