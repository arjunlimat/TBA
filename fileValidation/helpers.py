"""
helper functions are defined here.
"""
import json
from datetime import datetime

from rest_framework.status import HTTP_200_OK
from rest_framework.exceptions import APIException

from utilities.logman import logman

LOGGER = logman("TBASourceMatcher")

FAILED_TO_GET_RESPONSE = "Failed to get response"
FAILED_TO_CONNECT = "Failed to connect"
ERROR_MSG_FILE_REPORT = "Unable to get File/Report"

MAESTRO = {
    "default": {"description": "Expectation Failed", "title": "Failed"},
    "config": {"description": "Client config not present", "title": "Failed"},
    "inq_resp": {"description": "Unable to get response from TBA Inquiry", "title": FAILED_TO_GET_RESPONSE},
    "redis_connect": {"description": "Unable to connect Cache Storage", "title": FAILED_TO_CONNECT},
    "redis_response": {"description": ERROR_MSG_FILE_REPORT, "title": FAILED_TO_GET_RESPONSE},
    "empty_file": {"description": "File/Report key can't be empty or None", "title": "Failed to Process"},
    "inq_connect": {"description": "Unable to connect TBA Inquiry", "title": "Failed to Connect"},
    "rule_connect": {"description": "Unable to connect Rule Engine", "title": FAILED_TO_CONNECT},
    "rule_resp": {"description": "Unable to get response from Rule Engine", "title": FAILED_TO_GET_RESPONSE},
    "excel_connect": {"description": "Unable to connect Excel Formatter", "title": FAILED_TO_CONNECT},
    "update_connect": {"description": "Unable to connect TBA Update", "title": FAILED_TO_CONNECT},
    "update_resp1": {"description": "Got improper response from TBA Update", "title": "Failed to get proper response"},
    "update_resp2": {"description": "Unable to get response from TBA Update", "title": FAILED_TO_GET_RESPONSE},
    "identifier_mismatch": {"description": "None of the identifier have match field", "title": "Failed to process"},
    "not_valid": {"description": "Configurations seems invalid", "title": "Failed to process"},
}


def _default_error(source) -> dict:
    DEFAULT_ERROR = {
        "audit": {
            "uid": source.uid,
            "clientDet": source.client_id,
            "botName": source.plugin_name,
            "ticketId": "",
            "fileType": source.process_type,
            "flag": True,
            "allocatedBy": source.user_name,
            "processJobMappingId": source.pjm_id,
        },
        "processLog": [
            {
                "uid": source.uid,
                "processJobMappingId": source.pjm_id,
                "botId": "MFvsTBA",
                "elementType": "status",
                "value": "Failed",
            }
        ],
        "redisKeys": dict(source.redis_keys),
    }
    return DEFAULT_ERROR


class FileValidationError(APIException):
    status_code = HTTP_200_OK
    _json = {
        "status": "Failed",
        "statusMessage": "Expectation Failed",
        "overAllStatus": False,
    }

    def __init__(self, source, msg: str = None, maestro: str = "default", name: str = ""):

        self.default_code = "error"
        default_detail = _default_error(source)

        if msg:
            self._json.update({"statusMessage": msg})
        default_detail["audit"].update(
            {"createTimestamp": str(datetime.now().timestamp()), "fileName": name, "json": json.dumps(self._json)}
        )
        default_detail["processLog"][0].update(
            {"timestamp": str(datetime.now().replace(microsecond=0).isoformat()),}
        )
        default_detail.update({"maestro": MAESTRO[maestro]})
        default_detail.update(
            {**self._json, "noConfigStatusMessage": "",}
        )
        self.detail = default_detail


class ConfigError(FileValidationError):

    def __init__(self, msg: str):
        self.detail = {"status": "Failed", "statusMessage": msg}


def get_error_string(error) -> str:
    """Get serializer errors string value"""

    error_str = ""

    if isinstance(error, list):
        return f" {str(error[0])},"

    for key in error.keys():
        error_str += get_error_string(error[key])

    return error_str
