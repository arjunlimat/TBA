"""This module contain all logic functions"""
import json
import copy
import warnings
from io import BytesIO
from builtins import Exception
from datetime import datetime
import dateutil
from typing import Dict, List, Optional, Tuple
import random
from collections import defaultdict
from operator import itemgetter
import os

import pandas as pd
from django.conf import settings
from requests import Session
from requests.utils import quote
from py_zipkin.zipkin import create_http_headers_for_new_span

from .helpers import (
    LOGGER,
    FileValidationError,
)
from .effectivedate import dateproperformat

warnings.simplefilter(action="ignore", category=FutureWarning)

# Satisfied/Not Satisfied
MET = "Met"
NOT_MET = "Not Met"

# Corrective Actions
TBA_ADD = "TBA Add"
TBA_UPDATE = "TBA Update"
TBA_DELETE = "TBA Delete"
TBA_VALIDATE = "TBA Validate"
TBA_NOTICE_CANCEL = "TBA Notice Cancel"
TBA_NOTICE_UPDATE = "TBA Notice Update"
TBA_PENDEVNT_CANCEL = "TBA Pending Event Cancel"
TBA_PENDEVNT_UPDATE = "TBA Pending Event Update"
RERUN_EVENT = "Rerun-Event"
RERUN_EVENT_DELETE = "Rerun-Event Delete"
FILE_REPORT_UPDATE = "File/Report Update"

NO_MISMATCH = "No Mismatch"
HUMAN_IN_LOOP = "HumanInLoop"
NO_ACTION_IS_TAKEN = "no action is taken"

FAILED_TO_GET_RESPONSE = "Failed to get response"
FAILED_TO_CONNECT = "Failed to connect"
ERROR_MSG_TBAINQUIRY_CONNECT = "Unable to connect TBA Inquiry"
ERROR_MSG_FILE_REPORT = "Unable to get File/Report"
ERROR_MSG_PARTICIPANT_NOT_TBA = "All participants not in TBA"
ERROR_MSG_UNABLE_TO_CONNECT_REDIS = "Unable to connect Cache Storage"
ERROR_MSG_UNABLE_GET_RESPONSE_TBAINQUIRY = "Unable to get response from TBAInquiry"
ERROR_MSG_UNABLE_GET_RESPONSE_TBAUPDATE = "Unable to get response from TBA Update"

CORRECTIVE_ACTIONS = [
    TBA_ADD,
    TBA_UPDATE,
    TBA_VALIDATE,
    TBA_DELETE,
    RERUN_EVENT,
    RERUN_EVENT_DELETE,
    TBA_NOTICE_CANCEL,
    TBA_NOTICE_UPDATE,
    TBA_PENDEVNT_CANCEL,
    TBA_PENDEVNT_UPDATE,
]
COMPARE_TBA = ("compare with tba",)
COMPARE_REPORT = (
    "compare previous report",
    "compare with other report",
    "comparepreviousreport",
)
INTERNAL_ID = {
    "id": 0,
    "inquiryName": "Unmasked SSN/Taxpayer ID",
    "parNM": "",
    "panelId": "",
    "tbaFieldName": "internalId",
    "fieldType": "String",
    "jsonKey": "intnId",
    "subJsonKey": "",
    "metaData": "",
    "identifier": "",
    "recordIdentifier": "",
    "inquiryDefName": "UNMASKEDSSN_INTERNALID",
    "sequence": "1",
    "effDateType": "date",
    "effFromDate": '{"effectiveFromDateAppNameWithoutSpace":"","effectiveFromDateSheetName":"","effectiveFromDateRIdentifier":"","effectiveFromDateField":"","effectiveFromDatePeriod":"Current","effectiveFromDateInterval":"Date"}',
    "effToDate": '{"effectiveToDateAppNameWithoutSpace":"","effectiveToDateSheetName":"","effectiveToDateRIdentifier":"","effectiveToDateField":"","effectiveToDatePeriod":"","effectiveToDateFrequency":"","effectiveToDateInterval":""}',
    "rowMatrix": "",
    "columnMatrix": "",
}


def clean_pid(pid: str) -> str:
    """Remove special symbols from pid"""
    return "".join(filter(str.isalnum, pid))


def strip_pid(identifier, raw_pid: str) -> str:
    """Strip pid if more than nine digits"""
    pid = clean_pid(raw_pid)
    if identifier == "pid":
        if len(pid) >= 9:
            return str(pid[-9:])
        length_pid = len(pid)
        add_zero = "0" * (9 - length_pid)
        return str(add_zero + pid)
    return str(pid)


def get_fields(req_data, rerun_flag):
    """Function for getting update/rerun fields from request"""
    fields = None

    if rerun_flag:
        fields = [field for field in req_data if field["rerunFlag"].strip() == "Y"]
    else:
        fields = [field for field in req_data if field["rerunFlag"].strip() == "N"]

    return fields


def isdata(fields: list, name: str) -> bool:
    """
    Check if fields and name are not empty

    Args:
        fields (list): list of data
        name (str): string field

    Returns:
        bool: if both have some data
    """

    if len(fields) > 0 and name.strip("") != "":
        return True
    return False


def inkeys_equal(field: str, keys: list, val: str, eq: bool) -> bool:
    """
    Check if field in keys and equal to val

    Args:
        field (str): string value name
        keys (list): list of keys
        val (str): value to which compare
        eq (bool): equal or not equal

    Returns:
        bool: if in keys and equal
    """

    if field in keys and ((eq and field == val) or (not eq and field != val)):
        return True
    return False


def merge_keys(vals: tuple) -> str:
    """
    merge appName, sheetName and identifier from vals

    Args:
        vals (tuple): contain strings
    Returns:
        str: appName_sheetName_identifier if not empty
    """
    result = ""

    for val in vals[1:]:
        if val.strip() != "":
            result += val + "_"

    return result


def in_rule_fields(val: str, rule_list: List[tuple]) -> bool:
    """
    If file or tba matching field available in rule_fields return `True`

    Args:
        val (str): file or tba field name
        rule_list (List[tuple]): list of rule fields

    Returns:
        bool: True if exists, False otherwise
    """

    field_list = [field[0] for field in rule_list]

    if val in field_list:
        return True
    return False


def mask_ssn(value: List[dict]) -> List[dict]:
    """
    Mask ParticipantSsn and return the response

    >>> resp = [{"participantSsn": "123456789", ...}, {"participantSsn": "12-45-789", ...}]
    >>> mresp = mask_ssn(resp)
    >>> mresp
    >>> [{"participantSsn": "xxxxx6789", ...}, {"participantSsn": "xxxxx-789", ...}]
    """
    mval = lambda x: "xxxxx" + x[-4:]
    for val in value:
        val.update({"participantSsn": mval(val["participantSsn"])})

    return value


def combined_name(f_name: str, i_name: str, s_name: str) -> str:
    """
    Join f_name with i_name and s_name if not empty
    """
    key_name = f_name
    if i_name.strip() != "":
        key_name = key_name + "_" + i_name
    if s_name.strip() != "":
        key_name = key_name + "_" + s_name
    key_name = key_name + ".pkl"
    return key_name


def common_keys(l1: list, l2: list) -> list:
    """
    Get common fields
    """

    return list(set(l1).intersection(set(l2)))


class SourceMatch:
    """SourceMatch class is to call Redis, TBAInquiry and Rule Engine"""

    def __init__(self, request: dict, header_details):
        self.header_details = header_details
        self.ksd_config = request["ksdConfig"]  # For excel formatter
        self.process_feature_config = request["processFeatureConfig"]  # For excel formatter
        self.bot_output = request["botOutput"]  # For excel formatter
        self.process_job_mapping = request["processJobMapping"]
        self.client_id = request["clientId"]
        self.client_name = request["clientName"]
        self.ksd_file_details = request["ksdFileDetails"]
        self.file_formatter = request["File Formatter"]
        self.file_validator = request["File Validator"]
        self.uid = request["uid"]
        self.user_name = request["userName"]
        self.phase_id = request["phaseId"]
        self.plugin_name = request["pluginName"]
        self.create_time_stamp = request["createTimeStamp"]
        self.application_type = request["applicationType"]
        self.phase_names = request["phaseNames"]
        self.business_ops = request["businessOps"]
        self.process_name = request["processName"]
        self.process_type = request["processType"]
        self.business_unit = request["businessUnit"]
        self.job_name = request["jobName"]
        self.pjm_id = request["pjmId"]
        self.rules_config = request["rulesConfig"]
        self.tba_update_config = get_fields(request["tbaUpdateConfig"], rerun_flag=False)
        self.rerun_config = get_fields(request["tbaUpdateConfig"], rerun_flag=True)
        self.update_event_name = {event["updateName"]: event["eventName"] for event in request["tbaUpdateConfig"]}
        self.tba_match_config = request["tbaMatchConfig"]
        self.required_files: set = set()
        self.required_sheets: set = set()
        self.required_fields: set = set()
        self.required_identifier: set = set()
        self.file_identifier = defaultdict(set)
        self.inquiry_config = request["tbaInquiryConfig"]
        self.notice_config = request["tbaNoticeInqConfig"]
        self.tba_event_hist_inq_config = request["tbaEventHistInqConfig"]
        self.tba_pend_event_inq_config = request["tbaPendEventInqConfig"]
        self.ksd_output_file_details = request["ksdOutputFileDetails"]
        self.layout_config = request["layoutConfig"]
        self.redis_keys = request["redisKeys"]
        self.internal_id = dict()  # store internal id's with ssn as key
        self.audit = {
            "uid": self.uid,
            "clientDet": self.client_id,
            "botName": self.plugin_name,
            "ticketId": "",
            "fileType": self.process_type,
            "flag": True,
            "allocatedBy": self.user_name,
            "processJobMappingId": self.pjm_id,
        }
        self.failed_process_log = {
            "uid": self.uid,
            "processJobMappingId": self.pjm_id,
            "botId": "MFvsTBA",
            "elementType": "status",
            "value": "Failed",
        }
        self.ppt_total = 0
        self.ppt_verified = 0
        self.ppt_success = 0
        self.ppt_failed = 0
        self.excel_botoutput = ""
        self.errors = set()
    


    def in_inquiry(self, def_name: str, identifier: str, config: List[dict]):
        for field in config:
            if field["inquiryDefName"] == def_name and field["identifier"] == identifier:
                if "effDateType" in field.keys() and field["effDateType"] == "application":
                    self.ppt_specific_fields(field["effFromDate"], field["effToDate"])
                return True
        return False

    def inq_wout_identifier(self, def_name: str, config: List[dict], inq_def_name: str = "inquiryDefName"):
        for field in config:
            if field[inq_def_name] == def_name:
                return True
        return False

    # Look for the configured inquiry fields with proper identifier configurations
    def inquiry_lookup(self, def_name: str, identifier: str):
        """
        Look for the given def_name with it's identifier in inquiry fields.
        If not configured properly add error msg to `self.errors`.
        """
        if self.in_inquiry(def_name, identifier, self.inquiry_config) or self.in_inquiry(
            def_name, identifier, self.notice_config
        ):
            self.required_fields.add((def_name, identifier))
            return
        elif self.inq_wout_identifier(
            def_name, self.tba_event_hist_inq_config, "eventHistDefName"
        ) or self.inq_wout_identifier(def_name, self.tba_pend_event_inq_config, "pendgEvntDefName"):
            self.required_fields.add((def_name))
            return
        elif self.inq_wout_identifier(def_name, self.inquiry_config) or self.inq_wout_identifier(
            def_name, self.notice_config
        ):
            self.errors.add(f"{def_name} not configured with identifier '{identifier}' in TBA")
            return
        self.errors.add(f"{def_name} is not configured in TBA")
    
    def ppt_specific_fields(self, frm_date: str, to_date: str):
        eff_frm_date = json.loads(frm_date)
        eff_to_date = json.loads(to_date)
 
        if eff_frm_date["effectiveFromDateAppNameWithoutSpace"].lower() != 'tba' :
            self.required_files.add(eff_frm_date["effectiveFromDateAppNameWithoutSpace"].lower())
            self.required_sheets.add(eff_frm_date["effectiveFromDateSheetName"].lower())
            self.required_identifier.add(eff_frm_date["effectiveFromDateRIdentifier"])
            self.file_identifier[eff_frm_date["effectiveFromDateAppNameWithoutSpace"].lower()].add(
                    eff_frm_date["effectiveFromDateRIdentifier"])
 
        if eff_to_date["effectiveToDateAppNameWithoutSpace"].lower() != 'tba' :
            self.required_files.add(eff_to_date["effectiveToDateAppNameWithoutSpace"].lower())
            self.required_sheets.add(eff_to_date["effectiveToDateSheetName"].lower())
            self.required_identifier.add(eff_to_date["effectiveToDateRIdentifier"])
            self.file_identifier[eff_to_date["effectiveToDateAppNameWithoutSpace"].lower()].add(
                    eff_to_date["effectiveToDateRIdentifier"])

    def add_ppt_specific_fields(self, inq_def_name: str, inquiry_fields: List[dict]):
        """
        Collect participant specific fields used in eff(To/From)Date

        Args:
            inq_def_name (str): inquiry def name used in match config
            inquiry_fields (List[dict]): inquiry fields
        """

        inq_field = [field for field in inquiry_fields if field["inquiryDefName"] == inq_def_name]

        for field in inq_field:
            if field["effDateType"].lower() == "application":
                eff_frm_date = json.loads(field["effFromDate"])
                eff_to_date = json.loads(field["effToDate"])
                self.required_identifier.add(eff_frm_date["effectiveFromDateRIdentifier"])
                self.required_sheets.add(eff_frm_date["effectiveFromDateSheetName"].lower())
                self.required_files.add(eff_frm_date["effectiveFromDateAppNameWithoutSpace"].lower())
                self.file_identifier[eff_frm_date["effectiveFromDateAppNameWithoutSpace"].lower()].add(
                    eff_frm_date["effectiveFromDateRIdentifier"]
                )
                self.required_identifier.add(eff_to_date["effectiveToDateRIdentifier"])
                self.required_sheets.add(eff_to_date["effectiveToDateSheetName"].lower())
                self.required_files.add(eff_to_date["effectiveToDateAppNameWithoutSpace"].lower())
                self.file_identifier[eff_to_date["effectiveToDateAppNameWithoutSpace"].lower()].add(
                    eff_to_date["effectiveToDateRIdentifier"]
                )

    def add_actions_identifier(self, actions: list):
        """
        Collect identifier from actions if related to file
        """

        for action in actions:
            inner_actions = action["actions"]
            for inr_action in inner_actions:
                if inr_action["updateToRadio"].lower() == "field":
                    self.required_identifier.add(inr_action["updateToFileIdentifier"])
                    self.required_sheets.add(inr_action["updateToSheetName"].lower())
                    self.required_files.add(inr_action["updateToFileName"].lower())
                    self.file_identifier[inr_action["updateToFileName"].lower()].add(
                        inr_action["updateToFileIdentifier"]
                    )
                if inr_action.get("effectiveFromRadio", "").lower() == "field":
                    self.required_identifier.add(inr_action["effectiveFromFileIdentifier"])
                    self.required_sheets.add(inr_action["effectiveFromSheetName"].lower())
                    self.required_files.add(inr_action["effectiveFromFileName"].lower())
                    self.file_identifier[inr_action["effectiveFromFileName"].lower()].add(
                        inr_action["effectiveFromFileIdentifier"]
                    )

    def collect_required_fields(self, inquiry_fields: List[dict]):
        """
        Collect required fields from matchConfig and rulesConfig
        """

        for field in self.tba_match_config:
            dest_file_name = "tba"
            if field["matchType"].lower() in COMPARE_TBA:
                # self.required_fields.add((field["inquiryDefName"], field["identifier"]))
                self.inquiry_lookup(field["inquiryDefName"], field["identifier"])
                self.add_ppt_specific_fields(field["inquiryDefName"], inquiry_fields)
            elif field["matchType"].lower() in COMPARE_REPORT:
                dest_file_name = field["fileNameDest"]
                self.required_identifier.add(field["reportIdentifierDest"])
                self.required_sheets.add(field["sheetNameDestWoutSpace"].lower())
                self.required_files.add(field["fileNameDestWoutSpace"].lower())
                self.file_identifier[field["fileNameDestWoutSpace"].lower()].add(field["reportIdentifierDest"])
            self.required_files.add(field["fileNameWoutSpace"].lower())
            self.file_identifier[field["fileNameWoutSpace"].lower()].add(field["identifier"])
            self.required_identifier.add(field["identifier"])
            self.required_sheets.add(field["sheetNameWoutSpace"].lower())
            self.add_actions_identifier(json.loads(field["actions"]))
            if field["ruleName"].strip() != "":
                f_fields, t_fields = self.get_rules_fields(
                    {
                        "fileFieldName": field["mfFieldWoutSpace"],
                        "tbaFieldName": field["inquiryDefName"],
                        "ruleName": field["ruleName"] if field["ruleName"] != "NA" else "",
                    },
                    field["fileName"],
                    dest_file_name,
                )

                for t_field in t_fields:
                    tba_field = t_field[-1]
                    # self.required_fields.add((tba_field[0], tba_field[-1]))
                    self.inquiry_lookup(tba_field[0], tba_field[-1])
                    self.required_identifier.add(tba_field[-1])
                    self.required_sheets.add(tba_field[2].lower())
                    self.required_files.add(tba_field[1].lower())
                for f_field in f_fields:
                    file_field = f_field[-1]
                    self.required_identifier.add(file_field[-1])
                    self.required_sheets.add(file_field[2].lower())
                    self.required_files.add(file_field[1].lower())
                    self.file_identifier[file_field[1].lower()].add(file_field[-1])

    def filtered_fields(self, filter_fields: List[dict], inq_def_name: str) -> List[dict]:
        """
        Filter fields which are present in matchConfig and rulesConfig

        Args:
            filter_fields (List[dict]): fields to filter
        Returns:
            List[dict]: return filtered fields
        """
        filtered_list = list()

        LOGGER.info("Filtering required fields and identifiers", extra=self.header_details)
        if not self.required_fields:
            self.collect_required_fields(filter_fields)
            LOGGER.info(f"Required Inquiry fields: {self.required_fields}", extra=self.header_details)
            LOGGER.info(f"Required Identifier(s): {self.required_identifier}", extra=self.header_details)
        for field in filter_fields:
            if (field[inq_def_name], field["identifier"]) in self.required_fields:
                filtered_list.append(field)

        # NotImplemented -- add InqDefName without identifier to `self.required_fields`
        # for omitting identifier check on inquiry fields. It will run
        # `self.get_another_identifier_row` fallback loop when `tba_row` is `None`.

        return filtered_list

    def check_match_config(self):
        """
        check match config fields
        """

        for field in self.tba_match_config:
            fname = field["fileNameWoutSpace"]

            for ksdfile in self.ksd_file_details:
                _file = json.loads(ksdfile)
                if _file["fileNameWoutSpace"].lower() == fname.lower():
                    break
            else:
                self.errors.add("File name mismatch for few fields of Match TBA")

    def check_identifiers(self, identifiers: set, keys, name: str):

        redis_keys = keys
        if isinstance(keys, dict):
            redis_keys = keys["detailRedisKey"]

        for identifier in identifiers:
            for key in redis_keys:
                if key["identifier_name"] == identifier:
                    break
            else:
                self.errors.add(f"Redis key not found for {name} with identifier('{identifier}').")

    def check_file_identifier(self):
        """
        check file identifier
        """

        for namewoutspace, identifiers in self.file_identifier.items():
            name = None
            for ksdfile in self.ksd_file_details:
                _file = json.loads(ksdfile)
                if _file["fileNameWoutSpace"].lower() == namewoutspace.lower():
                    name = _file["fileName"]
                elif (
                    _file.get("prevReportFileNameWs") is not None
                    and _file.get("prevReportFileNameWs", "").lower() == namewoutspace.lower()
                ):
                    name = _file["prevReportFileName"]

            if name is None:
                raise FileValidationError(self, f"{namewoutspace}'s configurations not found.", maestro="not_valid")

            if name in self.file_validator.keys():
                self.check_identifiers(identifiers, self.file_validator[name], name)
            elif name in self.file_formatter.keys():
                self.check_identifiers(identifiers, self.file_formatter[name], name)
            else:
                raise FileValidationError(self, f"{name}'s redis keys not found.", maestro="not_valid", name=name)

    def convert_to_string(self, fields: List[dict]) -> List[dict]:
        """panelId to string"""

        _inquiry_fields = list()

        for field in fields:
            field["panelId"] = str(field["panelId"])
            _inquiry_fields.append(field)

        return _inquiry_fields

    def get_pptidentifier(
        self, file_name: str, ppt_identifier: str, layout_config: list, fields: dict
    ) -> Optional[str]:
        """
        Get mfFieldWoutSpace field from layout with respect to pptidentifier

        Args:
            file_name (str): name of the file
            ppt_identifier (str): ssn field of file from ksdFileDetails

        Returns:
            (str) mfFieldWoutSpace if found None otherwise
        """

        pptidentifier = None
        field_name = fields["fieldName"]
        field_name_wout_space = fields["fieldNameWoutSpace"]

        for layout in layout_config:
            if layout["fileName"] == file_name and layout[field_name] == ppt_identifier:
                pptidentifier = layout[field_name_wout_space]
                break

        if pptidentifier is None or len(pptidentifier) == 0 or pptidentifier.strip() == "":
            LOGGER.error("Identifier doesn't match with File/Report", extra=self.header_details)
            raise FileValidationError(
                self, "Identifier doesn't match with File/Report", maestro="identifier_mismatch", name=file_name
            )

        return pptidentifier

    def get_filtered_ppt_from_redis_frame(self, detail_redis_key: list):
        """
        get filetered participant from each detail redis key of a mainframe file.
        """

        list_of_participant_set = list()
        for redis_key in detail_redis_key:
            ppt_identifier = redis_key["ssn"]
            redis_frame = redis_key["required_frame"]
            redis_frame.fillna("", inplace=True)
            redis_frame.drop_duplicates(inplace=True)
            redis_frame = redis_frame[pd.notnull(redis_frame[ppt_identifier])]
            redis_frame[ppt_identifier] = redis_frame[ppt_identifier].map(str)
            redis_frame = redis_frame[redis_frame[ppt_identifier].apply(lambda x: x.strip()) != ""]
            list_of_participant_set.append(set(redis_frame[ppt_identifier]))
        LOGGER.info("Filtering participants", extra=self.header_details)
        common_ppt_list = list(set.intersection(*list_of_participant_set))
        ppt_ver = 5
        if "pptVerifyTba" in self.tba_match_config[0] and self.tba_match_config[0]["pptVerifyTba"] not in ("NA", ""):
            ppt_ver = int(self.tba_match_config[0]["pptVerifyTba"])
            LOGGER.info(f"PPTVerify given {ppt_ver}", extra=self.header_details)
        ppt_tot = len(common_ppt_list)
        if ppt_tot <= ppt_ver:
            ppt_ver = ppt_tot
        random_ppt_list = random.sample(common_ppt_list, ppt_ver)
        self.ppt_total += ppt_tot
        self.ppt_verified += ppt_ver
        for item in detail_redis_key:
            df = item["required_frame"]
            item["required_frame"] = df[df[ppt_identifier].isin(random_ppt_list)]

    def get_redis_keys(self, file_name):
        """
        [description]
        """
        if file_name in self.file_validator:
            return self.file_validator[file_name]
        elif file_name in self.file_formatter:
            return self.file_formatter[file_name]

        raise FileValidationError(f"{file_name} redis keys not found", maestro="not_valid")

    def get_ksdfiles_details(self) -> Tuple[List[dict], set, set, set]:
        """
        Get multiple filesDetails with respect to their redis keys and ssn.
        """
        files = set()
        sheets = set()
        files_type = set()
        files_details = list()
        phase_names = json.loads(self.phase_names)["SourceMatch"]
        LOGGER.info(f"SourceMatch files in phase_names: {phase_names}", extra=self.header_details)
        source_match_files = phase_names.split(",")
        LOGGER.info(f"ksd_files_details data: {self.ksd_file_details}", extra=self.header_details)

        for ksd_file in self.ksd_file_details:
            _detail = dict()
            _file = json.loads(ksd_file)
            if _file["fileNameWoutSpace"].lower() in self.required_files and _file["fileName"] in source_match_files:
                _detail["ssn"] = self.get_pptidentifier(
                    _file["fileName"],
                    _file["pptidentifier"],
                    self.layout_config,
                    {"fieldName": "mfFieldName", "fieldNameWoutSpace": "mfFieldWoutSpace"},
                )
                _detail["fileName"] = _file["fileName"]
                _detail["fileNameWoutSpace"] = _file["fileNameWoutSpace"]
                _detail["sheetName"] = _file["sheetName"]
                _detail["fileType"] = _file["fileType"]
                _detail["pptidentifierType"] = _file["pptidentifierType"]

                temp_files = list()
                temp_files.append(
                    {
                        "ssn": _detail["ssn"],
                        "fileName": _detail["fileName"],
                        "fileNameWoutSpace": _detail["fileNameWoutSpace"],
                        "sheetName": _detail["sheetName"],
                        "fileType": _detail["fileType"],
                        "pptidentifierType": _detail["pptidentifierType"],
                    }
                )
                if (
                    _file.get("prevReportFileName", None) is not None
                    and _file.get("prevReportFileNameWs", "").lower() in self.required_files
                ):
                    temp_files.append(
                        {
                            "ssn": _detail["ssn"],
                            "fileName": _file["prevReportFileName"],
                            "fileNameWoutSpace": _file["prevReportFileNameWs"],
                            "sheetName": _detail["sheetName"],
                            "fileType": _detail["fileType"],
                            "pptidentifierType": _detail["pptidentifierType"],
                        }
                    )

                for temp_file in temp_files:
                    _file_formatter = self.get_redis_keys(temp_file["fileName"])

                    if isinstance(_file_formatter, dict):
                        _file_formatter = _file_formatter["detailRedisKey"]

                    detail_redis_key = [
                        redis_key
                        for redis_key in _file_formatter
                        if redis_key["identifier_name"] in self.required_identifier
                        and redis_key["sheet_name"].lower() in self.required_sheets
                    ]
                    LOGGER.info(
                        f"Required redis keys for {temp_file['fileName']}: {detail_redis_key}",
                        extra=self.header_details,
                    )
                    for key in detail_redis_key:
                        files.add(temp_file["fileName"])
                        sheets.add(temp_file["sheetName"])
                        files_type.add(temp_file["fileType"])
                        detail = copy.deepcopy(temp_file)
                        detail.update({"identifierName": key["identifier_name"]})
                        detail.update({"sheetNameWoutSpace": key["sheet_name"]})
                        detail.update({"detailRedisKey": key["key"]})
                        LOGGER.info(
                            f"Fetching redis key: {key['key']} for file: {temp_file['fileName']}",
                            extra=self.header_details,
                        )
                        detail.update({"required_frame": self.fetch_file_redis(key["key"], temp_file["fileName"])})
                        files_details.append(detail)

        self.get_filtered_ppt_from_redis_frame(files_details)

        LOGGER.info("Extracted ksdFileDetails from request", extra=self.header_details)
        return (files_details, files, files_type, sheets)

    def set_file_redis(self, file_name, data):
        """Set key to redis"""

        session = Session()
        url = settings.REDIS_URL_SET

        try:
            LOGGER.info(
                f"Hitting cache storage at URL: {url} to set file",
                extra=self.header_details,
            )
            response = session.post(settings.REDIS_URL_SET, files=data, headers=dict(Referer=settings.REDIS_URL_SET))
        except Exception as err:
            LOGGER.error(ERROR_MSG_UNABLE_TO_CONNECT_REDIS + f"{repr(err)}", extra=self.header_details)
            raise FileValidationError(
                self,
                ERROR_MSG_UNABLE_TO_CONNECT_REDIS,
                maestro="redis_connect",
                name=file_name,
            )

        if response.status_code == 201:
            LOGGER.info("File set successfully", extra=self.header_details)
            return response.json()
        LOGGER.error(f"Unable to get File/Report {response.content}", extra=self.header_details)
        raise FileValidationError(self, ERROR_MSG_FILE_REPORT, maestro="redis_response", name=file_name)

    def fetch_file_redis(self, rediskey: str, file_name: str) -> pd.DataFrame:
        """Try to fetch file from redis"""

        if rediskey != "" and rediskey is not None:
            session = Session()
            payload = {"key": quote(rediskey, safe="")}
            headers = create_http_headers_for_new_span()
            try:
                LOGGER.info(
                    f"Hitting cache storage at URL: {settings.REDIS_URL} with payload: {payload}",
                    extra=self.header_details,
                )
                response = session.get(url=settings.REDIS_URL, params=payload, headers=headers)
            except Exception as err:
                LOGGER.error(ERROR_MSG_UNABLE_TO_CONNECT_REDIS + f"{repr(err)}", extra=self.header_details)
                raise FileValidationError(
                    self, ERROR_MSG_UNABLE_TO_CONNECT_REDIS, maestro="redis_connect", name=file_name
                )

            if response.status_code == 200:
                redis_data_frame = pd.read_pickle(BytesIO(response.content), compression="zip")
                LOGGER.info("File fetched successfully", extra=self.header_details)
                return redis_data_frame

            LOGGER.error(f"Unable to get File/Report {response.content}", extra=self.header_details)
            raise FileValidationError(self, ERROR_MSG_FILE_REPORT, maestro="redis_response", name=file_name)

        LOGGER.error(f"File/Report key can't be empty or None: {rediskey}", extra=self.header_details)
        raise FileValidationError(self, "File/Report key can't be empty or None", maestro="empty_file", name=file_name)

    def get_fields_to_match(self, file_name: str, identifier_name: str) -> tuple:
        """Get fields to match from request"""

        fields_to_match = list()
        mf_field_track = dict()
        for match_field in self.tba_match_config:
            if match_field["fileName"] == file_name and match_field["identifier"] == identifier_name:
                fields_to_match.append(match_field)
                mf_field_track[match_field["mfFieldWoutSpace"]] = match_field["mfFieldName"]
        return (fields_to_match, mf_field_track)

    def get_fields_to_inquire(self, identifier_name: str, inquiry_fields: List[dict]) -> List[dict]:
        """Get fields to inquire from request"""

        fields_to_inquire = list()

        for inquiry_field in inquiry_fields:
            if inquiry_field["identifier"].strip() == identifier_name:
                fields_to_inquire.append(inquiry_field)

        return fields_to_inquire

    def call_tba_inquiry(self, inquiry_data: List[dict], files) -> list:
        """Call TBA Inquiry bot and get the participants data"""

        session = Session()
        payload = {
            "secretEngine": {
                "callbackUrl": "",
                "credentialUrl": "",
                "username": "",
                "password": "",
            },
            "tbaUrl": {"url": settings.TBA_API_URL},
            "inquiryData": [inquiry_data],
        }
        response = None
        if len(inquiry_data["participants"]) == 0 or not any(
            len(inquiry_data[inq]) > 0
            for inq in (
                "TBA",
                "tbaNoticeInqConfig",
                "tbaPendEventInqConfig",
                "tbaEventHistInqConfig",
            )
        ):
            return [list(), list()]
        try:
            headers = create_http_headers_for_new_span()
            LOGGER.info(f"Hitting TBA Inquiry at URL: {settings.TBA_INQUIRY_URL}", extra=self.header_details)
            response = session.post(url=settings.TBA_INQUIRY_URL, json=payload, headers=headers)
        except Exception as err:
            LOGGER.error(ERROR_MSG_TBAINQUIRY_CONNECT + f"{repr(err)}", extra=self.header_details)
            raise FileValidationError(self, ERROR_MSG_TBAINQUIRY_CONNECT, maestro="inq_connect", name=files)

        if response and response.status_code == 200:
            LOGGER.info("Got Response from Inquiry", extra=self.header_details)
            return response.json()

        LOGGER.error(ERROR_MSG_UNABLE_GET_RESPONSE_TBAINQUIRY + f" {response.content}", extra=self.header_details)
        raise FileValidationError(self, ERROR_MSG_UNABLE_GET_RESPONSE_TBAINQUIRY, maestro="inq_resp", name=files)

    def normalize_date(self, date_string, date_format):
        if len(date_string.strip()) == 8 and ("/" or "-") in date_string:
            _date_string = dateutil.parser.parse(date_string)
            if _date_string.year > 2000:
                _date_string = _date_string.replace(year=_date_string.year - 100)
                _date_string.strftime(date_format)
        return _date_string

    def get_date_conversion(self, date_string: str, file_name: str, eff_field: str) -> str:
        """
        Get date converted field
        date_string: str -> date from file
        file_name: str -> file name
        eff_field: str -> mfFieldWoutSpace field name
        """
        common_format = "%Y-%m-%d"
        layout_df = pd.DataFrame(self.layout_config)
        date_field = layout_df[(layout_df["fileName"] == file_name) & (layout_df["mfFieldWoutSpace"] == eff_field)]

        if not date_field.empty:
            field = date_field.iloc[0]

            date_value = None
            try:
                date_format = field["recordFormat"].lower()

                if "x(8)" in date_format:
                    date_format = "%Y%m%d"

                elif "x(10)" in date_format:
                    date_format = common_format

                elif "cc" in date_format:
                    date_format = date_format.replace("cc", "yy")
                date_format = date_format.replace("yyyy", "%Y")
                date_format = date_format.replace("mm", "%m")
                date_format = date_format.replace("dd", "%d")
                if date_string.strip() != "":
                    date_value = self.normalize_date(date_string, date_format)
                else:
                    date_value = datetime.today()

                return date_value.strftime(common_format)
            except Exception:
                LOGGER.error(f"field type is not date for {eff_field}", extra=self.header_details)
                date_value = datetime.today()
                return date_value.strftime("%Y-%m-%d")

    def get_ppt_row(self, ppt_id: str, fname: str, sname: str, iname: str, field_name: str):
        """
        Get identifier specific row for participant

        Args:
            ppt_id (str): participant id (SSN)
            fname (str): file name
            sname (str): sheet name
            iname (str): identifier name
            field_name (str): field name

        Returns:
            [description]
        """
        for ksd_file in self.ksdfiles_details:
            if (
                ksd_file["fileNameWoutSpace"] == fname
                and ksd_file["sheetNameWoutSpace"] == sname
                and ksd_file["identifierName"] == iname
            ):
                redis_frame = ksd_file["required_frame"]
                req_row = redis_frame[redis_frame[ksd_file["ssn"]] == ppt_id]

                return req_row

    def get_participants(
        self,
        ssn: str,
        file_name: str,
        participants: list,
        identifier_type: str,
        redis_frame: pd.DataFrame,
        inquiry_fields: List[dict],
    ):
        """
        Get participants and participant specif fields
        ssn: str -> pid column name
        file_name: str -> file name
        paritcipants: list -> participants list
        identifier_type: str -> identifier (pid, eid, iid)
        redis_frame: pd.DataFrame -> file data
        inquiry_fields: List[dict] -> self.inquiry_config of particular identifier
        """

        eff_from_fields = list()
        eff_to_fields = list()

        for inq_field in inquiry_fields:
            if "effDateType" in inq_field.keys() and inq_field["effDateType"].lower() == "application":
                eff_frm_field = json.loads(inq_field["effFromDate"])
                eff_to_field = json.loads(inq_field["effToDate"])
                eff_from_fields.append(
                    (
                        eff_frm_field["effectiveFromDateAppNameWithoutSpace"],
                        eff_frm_field["effectiveFromDateSheetName"],
                        eff_frm_field["effectiveFromDateRIdentifier"],
                        eff_frm_field["effectiveFromDateField"],
                    )
                )
                eff_to_fields.append(
                    (
                        eff_to_field["effectiveToDateAppNameWithoutSpace"],
                        eff_to_field["effectiveToDateSheetName"],
                        eff_to_field["effectiveToDateRIdentifier"],
                        eff_to_field["effectiveToDateField"],
                    )
                )

        for eff_field in eff_from_fields:
            for participant in participants:
                row = self.get_ppt_row(participant[identifier_type], *eff_field)

                if eff_field[-1] in row.columns.tolist():
                    value = row.iloc[0][eff_field[-1]]
                    participant.update({str(eff_field[-1]): self.get_date_conversion(value, file_name, eff_field[-1])})
                else:
                    LOGGER.warning(f"effFromDate field not in File/Report {eff_field[-1]}", extra=self.header_details)
        return participants

    def add_internal_id_inquiry(self, client_id: str, identifier: str, file_name: str) -> List[dict]:
        """
        Add internal ID field to TBA in inquiry payload
        """
        info_path = os.path.join(os.getcwd(), "fileValidation", "static", "internal_info.json")
        with open(info_path, "r") as internal:
            internal_info = json.load(internal)

        if client_id not in internal_info.keys() and str(int(client_id)) not in internal_info.keys():
            raise FileValidationError(
                self, f"{client_id} ID not configured for internal id", maestro="config", name=file_name
            )

        try:
            info = internal_info[client_id]
        except KeyError:
            info = internal_info[str(int(client_id))]
        INTERNAL_ID.update(
            {
                "inquiryName": info["inquiry_name"],
                "parNM": info["par_nm"],
                "panelId": str(info["panel_id"]),
                "identifier": identifier,
            }
        )
        return [INTERNAL_ID]

    def add_internal_id(self, response: List[dict]):
        """
        Add internal Id to response
        """
        for resp in response:
            resp.update({"internalId": self.internal_id.get(resp["participantSsn"], "")})

    def pick_internal_id(self, ppt_id: str, keys: list, data: dict):
        """
        Pick internal Id from inquiry response
        """
        inq_def_name = INTERNAL_ID["inquiryDefName"]
        if inq_def_name in keys:
            self.internal_id.update({str(ppt_id): data[inq_def_name]})

    def get_tba_inquiry_payload(
        self,
        file_name: str,
        file_type: str,
        redis_frame: pd.DataFrame,
        identifier_name: str,
        redis_pid_name: str,
        identifier_type: str,
    ) -> Tuple[list, dict]:
        """Generate request for inqiury"""

        participants = list()
        participant_list = list()
        _tba_inquiry_fields = self.get_fields_to_inquire(identifier_name, self.tba_inquiry_config)
        tba_notices_fields = self.get_fields_to_inquire(identifier_name, self.tba_notice_inq_config)

        tba_inquiry_fields = self.convert_to_string(_tba_inquiry_fields)
        identifier = str(identifier_type).lower()

        for participant in redis_frame[redis_pid_name]:
            if (participant != "") and (participant is not None) and (participant not in participant_list):
                participants.append({identifier: participant})
                participant_list.append(participant)

        for ppt in self.get_participants(
            ssn=redis_pid_name,
            file_name=file_name,
            participants=participants,
            identifier_type=identifier,
            redis_frame=redis_frame,
            inquiry_fields=_tba_inquiry_fields,
        ):
            ppt.update({identifier: strip_pid(identifier, ppt[identifier])})

        payload = {
            "clientId": str(int(self.client_id)),
            "fileName": file_name,
            "businessOps": self.business_ops,
            "processtype": self.process_name,
            "filetype": self.process_type,
            "buisnessunit": self.business_unit,
            "jobName": self.job_name,
            "inquiry": {"fields": [], "date": ""},
            "TBA": [dict(inq) for inq in tba_inquiry_fields],
            "tbaNoticeInqConfig": [dict(inq) for inq in tba_notices_fields],
            "tbaPendEventInqConfig": [dict(inq) for inq in self.tba_pend_event_inq_config],
            "tbaEventHistInqConfig": [dict(inq) for inq in self.tba_event_hist_inq_config],
        }
        # Add internal id
        if len(self.required_fields) > 0:
            payload["TBA"].extend(self.add_internal_id_inquiry(self.client_id, identifier_name, file_name))
        LOGGER.info(f"Inquiry payload for {file_name} - {identifier_name}: {payload}", extra=self.header_details)
        payload.update({"participants": participants})

        return (participant_list, payload)

    def get_prefix_and_fields(self, keys, condition, json_wout, index, t_field, f_field) -> None:
        """Get prefix and fields for rules"""

        temp_list = [condition[keys["field"]], json_wout[index][keys["field"]]]
        temp_prefix = (
            condition[keys["field"]],
            json_wout[index][keys["app"]],
            json_wout[index][keys["sheet"]],
            json_wout[index][keys["identifier"]],
        )

        temp_list.append(temp_prefix)

        if condition[keys["app"]].lower() == keys["destName"].lower():
            t_field.append(tuple(temp_list))
        if condition[keys["app"]].lower() == keys["fileName"].lower():
            f_field.append(tuple(temp_list))

    def process_varop_json(
        self, varop_json: list, varop_json_wout: list, file_name: str, dest_name: str
    ) -> Tuple[list, list]:
        """
        Process Var Operation json from rules and fetch required details

        Args:
            varop_json (list): variable operation josn of rule
            varop_json_wout (list): variable operation json without space of same rule
            file_name (str): file name of the field on which rule is defined
            dest_name (str): destination file/tba name to match with

        Returns:
            Tuple[list, list]: file and tba fields with rule details
        """
        t_field = list()
        f_field = list()

        for index, variable in enumerate(varop_json):
            if variable["varRadio"].lower() == "varapplicationvalue":
                keys = {
                    "field": "varField",
                    "app": "varApplication",
                    "sheet": "varSheetName",
                    "identifier": "varRecordIdentifier",
                    "destName": dest_name.lower(),
                    "fileName": file_name.lower(),
                }

                self.get_prefix_and_fields(keys, variable, varop_json_wout, index, t_field, f_field)

        return (f_field, t_field)

    def process_json_val(
        self, json_val: list, json_wout_val: list, file_name: str, dest_name: str
    ) -> Tuple[list, list]:
        """
        Process Json from rules and fetch required details

        Args:
            json_val (list): list of json fields
            json_wout_val (list): list of json without space fields
            file_name (str): file name of the field on which rule is defined
            dest_name (str): destination file/tba name to match with

        Returns:
            Tuple[list, list]: file and tba fields with rule details
        """
        t_field = list()
        f_field = list()

        for index, condition in enumerate(json_val["conditions"]):

            if condition["resultVariableRadio"].lower() == "application":
                keys = {
                    "field": "field",
                    "app": "appName",
                    "sheet": "sheetName",
                    "identifier": "recordIdentifier",
                    "destName": dest_name.lower(),
                    "fileName": file_name.lower(),
                }
                self.get_prefix_and_fields(keys, condition, json_wout_val, index, t_field, f_field)

            if condition["radio"].lower() == "field":
                keys = {
                    "field": "value",
                    "app": "valueAppName",
                    "sheet": "valueSheetName",
                    "identifier": "valueRecordIdentifier",
                    "destName": dest_name.lower(),
                    "fileName": file_name.lower(),
                }
                self.get_prefix_and_fields(keys, condition, json_wout_val, index, t_field, f_field)

        return (f_field, t_field)

    def get_rules_fields(self, key: dict, file_name: str, dest_name: str):
        """
        Get the rules fields

        Args:
            key (dict): contains rule name
            file_name (str): source file name to match
            dest_name (str): destination file name to match
        Returns:
            Tuple[list, list]: source & dest file/tba rules
        """

        tba_fields = list()
        file_fields = list()

        for rule in self.rules_config:
            if (
                rule["rulesDefinitions"][0]["validationType"]["valTypeName"].lower() == "business"
                and rule["rulesDefinitions"][0]["ruleName"] == key["ruleName"]
            ):
                json_val_full = json.loads(rule["rulesDefinitions"][0]["json"])
                json_wout_val_full = json.loads(rule["rulesDefinitions"][0]["jsonWoutName"])
                for index_full, json_val in enumerate(json_val_full):
                    json_wout_val = json_wout_val_full[index_full]["conditions"]
                    f_field, t_field = self.process_json_val(json_val, json_wout_val, file_name, dest_name)
                    file_fields.extend(f_field)
                    tba_fields.extend(t_field)

                varop_json = json.loads(rule["rulesDefinitions"][0]["varOperationJson"])[0]["variableRowOp"]
                varop_json_wout = json.loads(rule["rulesDefinitions"][0]["varOperationJsonWoutSpace"])[0][
                    "variableRowOp"
                ]
                f_field, t_field = self.process_varop_json(varop_json, varop_json_wout, file_name, dest_name)
                file_fields.extend(f_field)
                tba_fields.extend(t_field)

        return list(set(file_fields)), list(set(tba_fields))

    def get_corrective_action(self, action: str, satisfied: str, status: str) -> Tuple[str, str]:
        """
        Corrective action applicable based on status and satisfied

        Args:
            action (str): action if applicable
            satisfied (str): met/ not met or empty
            status (str): failed/success

        Returns:
            Tuple[str, str]: action we can apply and actionStatus
        """

        if (satisfied == MET and status == "Failed") or (satisfied == NOT_MET and status == "Success"):
            return (action, NO_ACTION_IS_TAKEN)
        elif satisfied.strip() == "" and status == "Success":
            return (action, NO_ACTION_IS_TAKEN)
        else:
            return (action, "")

    def get_tba_report_field(self, match_type: str, match_field: dict) -> str:
        """
        Get `tbaFieldName` from match_field based on `matchType`

        Args:
            match_type (str): matching type can be from COMPARE_REPORT or COMPARE_TBA
        Returns:
            str: field name either from tba or from report
        """

        if match_type.lower() in COMPARE_REPORT:
            return match_field["mfFieldNameDest"]
        else:
            return match_field["tbaFieldName"]

    def check_action(self, corrective_action: list, actions: tuple, action: dict, key: str):
        """
        switch case for checking all actions with respect to corrective action
        """
        if any(correct_act in actions for correct_act in corrective_action):
            return action[key]
        return ""

    def is_cond_available(self, actions: list, field: dict, action_status: str):

        action_list = list()

        if action_status == "Failed":
            for act in actions:
                if field["conditionName"] == act["condition"] and act["satisfied"] == NOT_MET:
                    action_list.append(act)
            if action_list:
                return action_list
            for act in actions:
                if field["conditionName"] == act["condition"] and act["satisfied"] == MET:
                    action_list.append(act)
            return action_list
        if action_status == "Success":
            for act in actions:
                if field["conditionName"] == act["condition"] and act["satisfied"] == MET:
                    action_list.append(act)
            if action_list:
                return action_list
            for act in actions:
                if field["conditionName"] == act["condition"] and act["satisfied"] == NOT_MET:
                    action_list.append(act)
            return action_list

    def get_participant_details(
        self,
        field: dict,
        ppt_id: str,
        match_config: dict,
        action_status: str,
    ) -> list:
        """
        Collect details of participant failed/success

        Args:
            field (dict): failed/success field
            ppt_id (str): participant id
            match_config (dict): match_config related to every field to match
            action_status (str): status for actionStatus

        Returns:
            list: containing all the required details
        """

        eff_date = ""
        event_name = ""
        rerun_event = ""
        notice_cancel = list()
        notice_update = ""
        if_condition = ""
        pendevnt_name = ""
        match_field = match_config[field["uniq"]]

        field_resp = list()

        _actions = json.loads(match_field["actions"])
        cmn_value = {
            "id": field["id"],
            "uid": self.uid,
            "participantSsn": ppt_id,
            "participantName": "",
            "fileName": match_field["fileName"],
            "sheetName": match_field["sheetName"],
            "dataMismatch": match_field["mfFieldName"],
            "tbaFieldName": self.get_tba_report_field(match_field["matchType"], match_field),
            "mainframeValue": field["fileFieldValue"],
            "tbaValue": field["tbaFieldValue"],
            "ruleName": field["ruleName"],
            "ruleFailedOnField": [match_field["mfFieldName"]],
            "reason": field["reason"],
            "eventName": event_name,
            "rerunEvent": rerun_event,
            "noticeCancel": notice_cancel,
            "noticeUpdate": notice_update,
            "pendingEventName": pendevnt_name,
            "effectiveDate": eff_date,
            "resultsVarable": field.get("resultsVarable", list()),
            "matchType": match_field["matchType"],
        }

        actions = self.is_cond_available(_actions, field, action_status)

        if actions:
            for action in actions:
                condition_name = action["condition"]
                if_condition = action["satisfied"]
                correct_action, status = self.get_corrective_action(
                    action["correctAction"], if_condition, action_status
                )

                field_value = dict(cmn_value)
                field_value.update(
                    {
                        "correctiveAction": [correct_action],
                        "conditionName": [condition_name],
                        "ifCondition": if_condition,
                        "actionStatus": status,
                        "updateAction": action.get("actions", []),
                    }
                )
                if len(action["actions"]) == 0:
                    field_resp.append(field_value)

                for act in action["actions"]:
                    action_value = dict(field_value)
                    event_name = act.get("eventName", "")
                    eff_date = act.get("effectiveFromDate", "")

                    rerun_event = self.check_action([correct_action], (RERUN_EVENT,), act, "reRunEvent")
                    notice_cancel = self.check_action([correct_action], (TBA_NOTICE_CANCEL,), act, "tbaNoticeCancel")
                    notice_update = self.check_action([correct_action], (TBA_NOTICE_UPDATE,), act, "noticeUpdate")
                    pendevnt_name = self.check_action(
                        [correct_action],
                        (
                            TBA_PENDEVNT_CANCEL,
                            TBA_PENDEVNT_UPDATE,
                        ),
                        act,
                        "pendingEventName",
                    )
                    action_value.update(
                        {
                            "eventName": event_name,
                            "effectiveDate": eff_date,
                            "rerunEvent": rerun_event,
                            "noticeCancel": notice_cancel,
                            "noticeUpdate": notice_update,
                            "pendingEventName": pendevnt_name,
                        }
                    )
                    field_resp.append(action_value)

        else:
            for action in _actions:
                cnd_not_found = dict(cmn_value)
                condition_name = action["condition"]
                if_cond = action["satisfied"]
                correct_action, status = self.get_corrective_action(action["correctAction"], if_cond, action_status)
                cnd_not_found.update(
                    {
                        "correctiveAction": [correct_action],
                        "conditionName": [condition_name],
                        "ifCondition": if_cond,
                        "actionStatus": status,
                        "updateAction": action.get("actions", []),
                    }
                )
                field_resp.append(cnd_not_found)

        return field_resp

    def get_participant_mismatch_success(
        self,
        match_config: dict,
        participant: dict,
    ) -> Tuple[list, list]:
        """
        Get mismatch or success participant details

        Args:
            match_config (dict): match_config related to every field to match
            participant (dict): participant from rule engine response

        Returns:
            Tuple[list, list]: tuple with mismatch and success details of ppt
        """

        mismatch_data = list()
        success_data = list()
        ppt_id = participant["participantId"]

        for failed_rule in participant["failedRules"]:
            mismatch_data.extend(
                self.get_participant_details(failed_rule, ppt_id, match_config, action_status="Failed")
            )
        for success_rule in participant["successRules"]:
            success_data.extend(
                self.get_participant_details(success_rule, ppt_id, match_config, action_status="Success")
            )

        return (mismatch_data, success_data)

    def update_sm_details(self, sm_details: list, change_sm: dict):
        """
        Update source matcher keys names

        Args:
            sm_details (list): list of source matcher details
            change_sm (list): list of fields name to change
        """
        for key in sm_details:
            if str(key["id"] + "__" + key["fileFieldName"]) in change_sm.keys():
                key.update({"fileFieldName": change_sm[key["id"] + "__" + key["fileFieldName"]]})
            if str(key["id"] + "__" + key["tbaFieldName"]) in change_sm.keys():
                key.update({"tbaFieldName": change_sm[key["id"] + "__" + key["tbaFieldName"]]})
            key.pop("destFlag", None)

    def get_sm_details(self, identifier_name: str, file_name: str, id_match: dict) -> List[dict]:
        """
        Get source matcher details with respect to identifier name and file name

        Args:
            identifier_name (str): identifier name given in request
            file_name (str): file name given in request

        Returns:
            List[dict]: source match details of rule request
        """
        sm_details = list()
        tba_match_config, mf_field_names = self.get_fields_to_match(file_name, identifier_name)

        for field in tba_match_config:
            dest_flag = "tba"
            if field["matchType"].lower() in COMPARE_TBA:
                id_match.update({str(field["id"]): field})
                file_field_name = field["mfFieldWoutSpace"]
                inquiry_def_name = field["inquiryDefName"]
            elif field["matchType"].lower() in COMPARE_REPORT:
                id_match.update({str(field["id"]): field})
                file_field_name = field["mfFieldWoutSpace"]
                inquiry_def_name = field["mfFieldWoutSpaceDest"]
                dest_flag = field["fileNameDest"] + "__" + field["sheetNameDest"] + "__" + field["reportIdentifierDest"]

            source = {
                "id": str(field["id"]),
                "fileFieldName": file_field_name,
                "actualField": file_field_name,
                "tbaFieldName": inquiry_def_name,
                "ruleName": field["ruleName"] if field["ruleName"] != "NA" else "",
                "destFlag": dest_flag,
            }
            sm_details.append(source)

        return sm_details

    def get_another_field(self, ppt_id: str, rule_field: tuple, file_details: List[dict]) -> str:
        """
        Get another file field for this configuration of rules
        """

        identifier = rule_field[-1]
        file_name = rule_field[1]

        row = None

        for file_detail in file_details:
            if file_detail["identifierName"] == identifier and (
                file_detail["fileName"] == file_name or file_detail["fileNameWoutSpace"] == file_name
            ):
                row: pd.DataFrame = file_detail["required_frame"]
                row = row[row[file_detail["ssn"]] == ppt_id]
                break

        if row is not None and not row.empty():
            return row[rule_field[0]]
        else:
            LOGGER.info(f"Unable to find participant in {file_name}", extra=self.header_details)

    def add_file_fields(
        self, ppt_id: str, rule_fields: List[tuple], field_name: str, row: pd.Series, comp: bool, **kwargs
    ) -> list:
        """
        Add file fields to the rule engine request field of participant

        Args:
            rule_fields (List[tuple]): rules fields defined for this field
            field_name (str): field name from the matchConfig
            row (pd.Series): row from the redis file data frame
            comp (bool): true if rule define on field_name, false otherwise

        Retruns:
            list: file fields
        """

        field = dict()
        fields = list()
        key_id = kwargs["id"]
        ksd_file = kwargs["ksdFile"]
        file_details = kwargs["fileDetails"]
        change_sm = kwargs["change_sm"]

        if not comp:
            field.update({field_name: row[field_name]})

        for rule_field in rule_fields:
            if comp and field_name == rule_field[0]:
                fields.insert(0, {"comp_element": merge_keys(rule_field[-1]) + rule_field[1]})
                change_sm.update({str(key_id + "__" + rule_field[0]): merge_keys(rule_field[-1]) + rule_field[1]})
            if rule_field[-1][-1] == ksd_file["identifierName"] and rule_field[-1][1] == ksd_file["fileNameWoutSpace"]:
                field.update({merge_keys(rule_field[-1]) + rule_field[1]: row[rule_field[0]]})
            else:
                field.update(
                    {
                        merge_keys(rule_field[-1])
                        + rule_field[1]: self.get_another_field(ppt_id, rule_field[-1], file_details)
                    }
                )

        fields.append(field)
        return fields

    def get_one_field(self, row, field_name):
        fields = list()

        for key, value in row.items():
            if field_name in key:
                for val in value:
                    if field_name in val.keys():
                        fields.append({k: v for k, v in val.items() if k == field_name})
                break

        return fields

    def get_another_identifier_row(self, ppt_id, identifier, file_details):

        tba_row = None

        for file_detail in file_details:
            if file_detail["identifierName"] == identifier:
                tba_frame = file_detail["tba_frame"]
                tba_row = tba_frame.get(ppt_id, None)
                break

        if tba_row is None:
            LOGGER.info(f"Unable to find participant in {identifier}", extra=self.header_details)

            for file_detail in file_details:
                tba_frame = file_detail["tba_frame"]
                if ppt_id in tba_frame.keys():
                    tba_row = tba_frame[ppt_id]
                    break
            else:
                err = mask_ssn([{"participantSsn": str(ppt_id)}])
                LOGGER.info(f"participant {err} not found", extra=self.header_details)

                raise FileValidationError(
                    self,
                    f"Participant with identifier: '{identifier}' not found in Inquiry response",
                    maestro="inq_resp",
                    name=file_name,
                )

        return tba_row

    def get_required_field_values(
        self, val: dict, rules_mapping: dict, comp: list, cmn_keys: list, fields: list, **kwargs
    ):
        """
        Same as collect required fields
        Added to remove cognitive compxty.
        """
        field_name = kwargs["field_name"]
        change_sm = kwargs["change_sm"]
        key_id = kwargs["id"]
        temp_dict = dict()
        for k, v in val.items():
            if k in cmn_keys and k in rules_mapping.keys():
                if comp[0] and k == field_name:
                    fields.insert(0, {"comp_element": rules_mapping[k]})
                    change_sm.update({str(key_id + "__" + k): rules_mapping[k]})
                    comp[0] = False
                temp_dict[rules_mapping[k]] = v
            elif k in cmn_keys and k not in rules_mapping.keys():
                temp_dict[k] = v
        if len(temp_dict) > 0:
            fields.append(temp_dict)

    def get_required_fields(self, req_fields, row, rules, comp_list: list, field_name, **kwargs):
        """
        Collect required fields
        """
        fields = list()
        key_id = kwargs["id"]
        change_sm = kwargs["change_sm"]
        rules_mapping = {field[0]: merge_keys(field[-1]) + field[1] for field in rules}

        for key, value in row.items():
            cmn_keys = common_keys(req_fields, key)

            for val in value:

                self.get_required_field_values(
                    val,
                    rules_mapping,
                    comp_list,
                    cmn_keys,
                    fields,
                    field_name=field_name,
                    change_sm=change_sm,
                    id=key_id,
                )

        return fields

    def add_tba_fields(self, ppt_id, rule_fields, field_name, row, comp, **kwargs):
        """
        Add tba fields
        """
        key_id = kwargs["id"]
        ksd_file = kwargs["ksdFile"]
        file_details = kwargs["fileDetails"]
        change_sm = kwargs["change_sm"]

        identifier_mapping = defaultdict(list)
        required_fields = list()

        for rule_field in rule_fields:
            identifier_mapping[rule_field[-1][-1]].append(rule_field)

        if not comp:
            identifier_mapping[ksd_file["identifierName"]].append((field_name, field_name, ("", "")))

        for identifier, fields in identifier_mapping.items():
            req_fields = [field[0] for field in fields]

            if identifier != ksd_file["identifierName"]:
                tba_row = self.get_another_identifier_row(ppt_id, identifier, file_details)
                temp_comp = False
                required_fields.extend(
                    self.get_required_fields(
                        req_fields, tba_row, fields, [temp_comp], field_name, change_sm=change_sm, id=key_id
                    )
                )
            else:
                required_fields.extend(
                    self.get_required_fields(
                        req_fields, row, fields, [comp], field_name, change_sm=change_sm, id=key_id
                    )
                )

        return required_fields

    def get_another_file_row(
        self, ppt_id: str, file_name: str, sheet_name: str, identifier_name: str
    ) -> Tuple[pd.Series, dict]:
        """
        Get destination file row for particular participant
        """
        for file_details in self.ksdfiles_details:
            if (
                file_details["fileName"] == file_name
                and file_details["sheetName"] == sheet_name
                and file_details["identifierName"] == identifier_name
            ):
                rdf_frame = file_details["required_frame"]
                rdf_row = rdf_frame[rdf_frame[file_details["ssn"]] == ppt_id]
                return rdf_row.iloc[0], file_details
        LOGGER.error(f"{file_name} destination file ({sheet_name, identifier_name}) don't have participant required")
        raise FileValidationError(
            self, "Comparison File/Report don't have common participant(s)", maestro="empty_file", name=file_name
        )

    def get_function(self, dest_flag: str):
        """
        Send function based on dest flag
        """
        if dest_flag == "tba":
            return self.add_tba_fields
        return self.add_file_fields

    def match_field(
        self, key: dict, redis_row: pd.Series, ksd_file: str, file_details: List[dict], change_sm: list
    ) -> Tuple[list, list]:
        """
        Process field from `tbaMatchConfig` and get file and tba field data
        """

        file_field: list = list()
        tba_field: list = list()
        key_id = key["id"]
        file_field_name = key["fileFieldName"]
        tba_field_name = key["tbaFieldName"]
        dest_flag = key["destFlag"]
        ppt_id: str = redis_row[ksd_file["ssn"]]
        if dest_flag == "tba":
            tba_row: dict = ksd_file["tba_frame"][ppt_id]
        else:
            f_name, s_name, i_name = dest_flag.split("__")
            tba_row, dest_ksd_file = self.get_another_file_row(ppt_id, f_name, s_name, i_name)
            dest_flag = f_name

        f_rule_fields, t_rule_fields = self.get_rules_fields(key, ksd_file["fileName"], dest_flag)

        ## Handling file part
        if f_rule_fields:
            if in_rule_fields(file_field_name, f_rule_fields):
                file_field.extend(
                    self.add_file_fields(
                        ppt_id,
                        f_rule_fields,
                        file_field_name,
                        redis_row,
                        True,
                        ksdFile=ksd_file,
                        fileDetails=file_details,
                        change_sm=change_sm,
                        id=key_id,
                    )
                )
            else:
                file_field.append({"comp_element": file_field_name})
                file_field.extend(
                    self.add_file_fields(
                        ppt_id,
                        f_rule_fields,
                        file_field_name,
                        redis_row,
                        False,
                        ksdFile=ksd_file,
                        fileDetails=file_details,
                        change_sm=change_sm,
                        id=key_id,
                    )
                )
        else:
            file_field.append({"comp_element": file_field_name})
            file_field.append({file_field_name: redis_row[file_field_name]})

        ## Handling dest file/tba part
        if t_rule_fields:
            tba_report_func = self.get_function(dest_flag)
            if in_rule_fields(tba_field_name, t_rule_fields):
                tba_field.extend(
                    tba_report_func(
                        ppt_id,
                        t_rule_fields,
                        tba_field_name,
                        tba_row,
                        True,
                        ksdFile=ksd_file,
                        fileDetails=file_details,
                        change_sm=change_sm,
                        id=key_id,
                    )
                )
            else:
                tba_field.append({"comp_element": tba_field_name})
                tba_field.extend(
                    tba_report_func(
                        ppt_id,
                        t_rule_fields,
                        tba_field_name,
                        tba_row,
                        False,
                        ksdFile=ksd_file,
                        fileDetails=file_details,
                        change_sm=change_sm,
                        id=key_id,
                    )
                )
        elif dest_flag == "tba":
            tba_field.append({"comp_element": tba_field_name})
            tba_field.extend(self.get_one_field(tba_row, tba_field_name))
        else:
            tba_field.append({"comp_element": tba_field_name})
            tba_field.append({tba_field_name: tba_row[tba_field_name]})

        return (file_field, tba_field)

    def get_file_tba_fields(self, ppt_id, sm_details, redis_row, ksdfile, ksdfile_deails, change_sm):

        file_fieldd = dict()
        tba_fieldd = dict()

        for key in sm_details:
            file_field, tba_field = self.match_field(key, redis_row, ksdfile, ksdfile_deails, change_sm)

            file_fieldd[key["id"]] = file_field
            tba_fieldd[key["id"]] = tba_field

        return {
            "type": "Participant" + str(self.pjm_id),
            "participantId": ppt_id,
            "fileFields": file_fieldd,
            "tbaFields": tba_fieldd,
        }

    def call_rule_engine(self, ksdfile_deails: List[dict], file_names: str) -> Tuple[list, list]:
        """
        Call Rule Engine
        """
        source_match_details = list()
        id_match_config = dict()
        participants = list()
        change_sm = dict()

        for ksdfile in ksdfile_deails:
            sm_details = self.get_sm_details(ksdfile["identifierName"], ksdfile["fileName"], id_match_config)
            redis_df = ksdfile["required_frame"]
            redis_df = redis_df[redis_df[ksdfile["ssn"]].isin(ksdfile["ppt_list"])]
            redis_df = redis_df.set_index(ksdfile["ssn"], drop=False)

            if len(sm_details) > 0:
                for index, redis_row in redis_df.iterrows():
                    participants.append(
                        self.get_file_tba_fields(index, sm_details, redis_row, ksdfile, ksdfile_deails, change_sm)
                    )

                source_match_details.extend(sm_details)

        self.update_sm_details(source_match_details, change_sm)
        session = Session()
        payload = {
            "pjmId": self.pjm_id,
            "phaseId": str(self.phase_id),
            "applicationType": "sourceMatch",
            "fileName": file_names,
            "sourceMatcherDetails": source_match_details,
        }
        LOGGER.info(f"Rule Engine Payload for {file_names}: {payload}", extra=self.header_details)
        payload.update({"participants": participants})

        try:
            headers = create_http_headers_for_new_span()
            headers["Content-Type"] = settings.CONTENT_TYPE
            LOGGER.info(f"Hitting Rule Engine at URL: {settings.RULE_ENGINE_URL}", extra=self.header_details)
            response = session.post(
                url=settings.RULE_ENGINE_URL,
                data=json.dumps(payload),
                headers=headers,
            )

        except Exception as err:
            LOGGER.error(f"Unable to connect Rule Engine {repr(err)}", extra=self.header_details)
            raise FileValidationError(self, "Unable to connect Rule Engine", maestro="rule_connect", name=file_names)

        if response and response.status_code == 200:
            LOGGER.info("Got Response from Rule Engine", extra=self.header_details)
            rule_response = response.json()
            mismatch_data = list()
            success_data = list()
            for participant in rule_response["participants"]:
                mismatch, success = self.get_participant_mismatch_success(id_match_config, participant)
                mismatch_data.extend(mismatch)
                success_data.extend(success)
            return (mismatch_data, success_data)
        else:
            LOGGER.error(f"Unable to get response from Rule Engine {response.content}", extra=self.header_details)
            raise FileValidationError(
                self, "Unable to get response from Rule Engine", maestro="rule_resp", name=file_names
            )

    def get_complete_request(self):
        """Add required/default keys in tba Update"""
        fields = list()

        for field in self.tba_update_config:
            kys = field.keys()
            if "processJobMapping" not in kys:
                field.update({"processJobMapping": ""})
            if "transId" not in kys:
                field.update({"transId": ""})
            if "value" not in kys:
                field.update({"value": ""})
            if "updatedDate" not in kys:
                field.update({"updatedDate": ""})
            if "updatedBy" not in kys:
                field.update({"updatedBy": ""})
            if "actLngDesc" not in kys:
                field.update({"actLngDesc": ""})

            fields.append(field)

        return fields

    def get_value(self, val, field: str) -> str:
        """
        Get proper string value corresponding to field

        Args:
            val (Union[list, dict]): it can be any list or dict
            field (str): string column name
        Returns:
            string value corresponding to field
        """

        if isinstance(val, list):
            for value in val:
                if isinstance(value, dict) and field in value.keys():
                    return value[field]
        elif isinstance(val, dict) and field in val.keys():
            return val[field]
        else:
            raise FileValidationError(self, f"{field} not found in Inquiry response", maestro="inq_resp")

    def get_field_value(
        self, file_name: str, sheet: str, identifier: str, field: str, file_details: List[dict], ppt: str
    ) -> str:
        """
        Get field value from TBA or file

        Args:
            file_name (str): TBA/file_name to pick data
            sheet (str): sheet name from file
            identifier (str): identifier of the file
            field (str): field name to pick data
            file_details (List[dict]): ksd details of files
            ppt (str): participant ssn

        Returns:
            str: value of the required field
        """

        value = ""

        for ksd_file in file_details:

            if file_name.lower() == "tba" and ksd_file["identifierName"] == identifier:
                required_file = ksd_file["tba_frame"]
                try:
                    required_row = required_file[ppt]
                    value = self.get_one_field(required_row, field)
                except KeyError:
                    LOGGER.error("PPT Id not found", extra=self.header_details)
                    value = ""
                    break

                if not isinstance(value, str) or not isinstance(value, datetime):
                    LOGGER.info("TBAUpdate -> TBA Value is not string", extra=self.header_details)
                    value = self.get_value(value, field)
                break
            elif (
                ksd_file["fileName"] == file_name
                and ksd_file["sheetName"] == sheet
                and ksd_file["identifierName"] == identifier
            ):
                required_file = ksd_file["required_frame"]
                required_row = required_file[required_file[ksd_file["ssn"]] == ppt]
                value = required_row.iloc[0][field]
                break
        else:
            value = "Error: Not Found"

        if isinstance(value, datetime):
            return str(value[:10])

        return value

    def get_field_date_value(
        self, action: dict, ksdfile_details: List[dict], ppt: str, result_var: List[dict]
    ) -> Tuple[str, str]:
        """
        Get field value and effective date based on
            - updateToRadio -> text, date, field (for field_value)
            - effectiveFromRadio -> text, date, field (for field_date)

        Args:
            action (dict): action details for getting date and field
            ksdfile_deails (List[dict]): ksd details for file
            ppt (str): participant ssn

        Returns:
            Tuple(str, str): field value and field date
        """
        field_value = None
        field_date = None

        if action["updateToRadio"] == "text":
            field_value = action["updateToText"]

        elif action["updateToRadio"] == "date":
            field_value = dateproperformat(action["updateToDate"], "CCYY-MM-DD")

        elif action["updateToRadio"] == "field":
            updt_file_name = action["updateToFileName"]
            updt_sheet = action["updateToSheetName"]
            updt_identifier = action["updateToFileIdentifier"]
            updt_file_field = action["updateToFileField"]

            field_value = self.get_field_value(
                updt_file_name, updt_sheet, updt_identifier, updt_file_field, ksdfile_details, ppt
            )
        elif action["updateToRadio"] == "resultVar":
            field_value = [
                field[action["updateToResult"]] for field in result_var if action["updateToResult"] in field.keys()
            ]
            if len(field_value) > 0:
                field_value = field_value[0]
            else:
                field_value = None

        if action["effectiveFromRadio"] == "text":
            field_date = action["effectiveFromText"]
        elif action["effectiveFromRadio"] == "date":
            field_date = action["effectiveFromDate"]
        elif action["effectiveFromRadio"] == "field":
            date_file_name = action["effectiveFromFileName"]
            date_sheet = action["effectiveFromSheetName"]
            date_identifier = action["effectiveFromFileIdentifier"]
            date_file_field = action["effectiveFromFileField"]

            field_date = self.get_field_value(
                date_file_name, date_sheet, date_identifier, date_file_field, ksdfile_details, ppt
            )

        ## TODO: if both are none then what should be done ???
        if field_value == "Error: Not Found":
            field_value = ""

        return (field_value, field_date)

    def action_for_condition(self, item: dict, condition_name: str, corrective_action: str) -> list:
        """
        Pull the action coresponding to conditon name, corrective action and ifCondition

        Args:
            item (dict): dictionary from the rule engine response
            condition_name (str): name of the conditon if defined on this item
            corrective_action (str): corrective action of the conditon if Met/Not Met

        Returns:
            list: list of actions for this item coresponding to condition name
        """

        match_field = [field for field in self.tba_match_config if str(field["id"]) == item["id"]][0]
        actions_for_field = json.loads(match_field["actions"])
        action_of_condtion = list()
        for action in actions_for_field:
            if (
                action["condition"] == condition_name
                and action["satisfied"] == item["ifCondition"]
                and action["correctAction"] == corrective_action
            ):
                action_of_condtion = action["actions"]
                break

        return action_of_condtion

    def update_payload(
        self,
        corrective_action: str,
        action: dict,
        payload: dict,
        identifier_type: str,
        **kwargs,
    ) -> None:
        """
        Update payload data for proper corrective action
        ## TODO: Other few corrective actions are pending

        Args:
            corrective_action (str): corrective action name
            action (dict): actions for this corrective action
            payload (dict): payload data dictionary
            identifier_type (str): PID, EID, IID which one?

        Kwargs:
            item (dict): success/failed details of participant
            ksdFileDetails (List[dict]): ksd details for files

        Returns:
            None
        """

        item = kwargs["item"]
        ksdfile_details = kwargs["ksdFileDetails"]
        ppt_ssn = item["participantSsn"]
        results_varable = item["resultsVarable"]

        for condition in action:
            if corrective_action in (TBA_ADD, TBA_UPDATE, TBA_VALIDATE, TBA_DELETE):
                update_action = [
                    field["tbaUpdateAction"]
                    for field in self.tba_update_config
                    if field["updateName"] == condition["eventName"]
                ][0]
                field_value, field_date = self.get_field_date_value(
                    condition, ksdfile_details, ppt_ssn, results_varable
                )
                payload["requestData"].append(
                    {
                        "identifier": ppt_ssn,
                        "identifier_type": identifier_type,
                        "efdt": dateproperformat(field_date, "CCYY-MM-DD"),
                        "tbaUpdateAction": update_action,
                        "field_value": {
                            str(item["eventName"]): "test" if update_action.lower() == "validate" else field_value,
                        },
                    }
                )
            elif corrective_action in (RERUN_EVENT,):
                fast_path = [
                    (field["actLngDesc"], field["eventName"], field["sequence"], field.get("overrideEdits", ""))
                    for field in self.tba_update_config
                    if field["updateName"] == condition["reRunEvent"]
                ][0]
                payload["rerun"].append(
                    {
                        "identifier": ppt_ssn,
                        "identifier_type": identifier_type,
                        "fastPath": fast_path[0],
                        "event name": fast_path[1],
                        "action": "Rerun",
                        "sequence": fast_path[2],
                        "overrideEdits": fast_path[3],
                    }
                )
            # Rerun-Event Delete
            elif corrective_action in (RERUN_EVENT_DELETE,):
                fast_path = [
                    (field["actLngDesc"], field["eventName"], field["sequence"], field.get("overrideEdits", ""))
                    for field in self.tba_update_config
                    if field["updateName"] == condition["reRunEvent"]
                ][0]
                payload["rerun"].append(
                    {
                        "identifier": ppt_ssn,
                        "identifier_type": identifier_type,
                        "fastPath": fast_path[0],
                        "event_name": fast_path[1],
                        "action": "Delete",
                        "sequence": fast_path[2],
                        "overrideEdits": fast_path[3],
                    }
                )
            elif corrective_action in (TBA_NOTICE_CANCEL,):
                for notice_cancel in condition["tbaNoticeCancel"]:
                    notice_name = [
                        field["noticeName"]
                        for field in self.tba_notice_inq_config
                        if field["inquiryDefName"] == notice_cancel
                    ][0]
                    payload["notice"].append(
                        {
                            "identifier": ppt_ssn,
                            "identifier_type": identifier_type,
                            "inquiryDefName": notice_cancel,
                            "form_name": notice_name,
                            "action": "Cancel",
                            "status_value": "",
                        }
                    )
            elif corrective_action in (TBA_NOTICE_UPDATE,):
                notice_update = condition["noticeUpdate"]
                notice_name = [
                    field["noticeName"]
                    for field in self.tba_notice_inq_config
                    if field["inquiryDefName"] == notice_update
                ][0]
                field_value, field_date = self.get_field_date_value(
                    condition, ksdfile_details, ppt_ssn, results_varable
                )
                payload["notice"].append(
                    {
                        "identifier": ppt_ssn,
                        "identifier_type": identifier_type,
                        "inquiryDefName": notice_update,
                        "form_name": notice_name,
                        "action": "Update",
                        "status_value": field_value,
                    }
                )
            elif corrective_action in (
                TBA_PENDEVNT_UPDATE,
                TBA_PENDEVNT_CANCEL,
            ):
                pendevnt_name = condition["pendingEventName"]
                evnt_act = corrective_action.split(" ")[-1].lower()
                fast_path = [
                    (evnt_act, field["eventName"], field["eventLongDesc"])
                    for field in self.tba_pend_event_inq_config
                    if field["pendgEvntDefName"] == pendevnt_name
                ][0]
                field_value, field_date = self.get_field_date_value(
                    condition, ksdfile_details, ppt_ssn, results_varable
                )

                payload["pendingEvents"].append(
                    {
                        "identifier": ppt_ssn,
                        "identifierType": identifier_type,
                        "inquiryDefName": pendevnt_name,
                        "action": fast_path[0],
                        "fastPath": fast_path[1],
                        "activity_long_description": fast_path[2],
                        "efDt": self.get_pendevnt_val(pendevnt_name, ppt_ssn),
                        "newEfDt": dateproperformat(field_date, "CCYY-MM-DD"),
                    }
                )

    def get_pendevnt_val(self, inq_name: str, ppt_id: str) -> str:
        """
        Get inq_name value from tba_frame
        """
        for ksd_file in self.ksd_file_details:
            if ppt_id in ksd_file["tba_frame"].keys():
                tba_row = ksd_file["tba_frame"][ppt_id]
                for key, val in tba_row.items():
                    cmn_keys = common_keys(inq_name, key)
                    if cmn_keys:
                        return val[0][inq_name]
        raise FileValidationError(self, "Participant not found in TBA response (Pending Event)")

    def tba_update_payload_data(
        self, rule_resp: List[dict], file_details: List[dict]
    ) -> Tuple[Dict[str, list], List[dict], List[dict]]:
        """
        Generate payload for different corrective action items

        Args:
            rule_resp (List[dict]): contains rule engine failed + success fields
            file_details (List[dict]): ksd details for every file with redis/tba frames

        Returns:
            Tuple[Dict[str, list], List[dict], List[dict]]: payload (requestData, notice, comment, rerun) data, used items and unused items
        """

        used_resp = list()
        unused_resp = list()
        payload_data = {
            "requestData": list(),
            "notice": list(),
            "pendingEvents": list(),
            "rerun": list(),
            "comment": list(),
        }

        for item in rule_resp:
            corrective_action = item["correctiveAction"][0]
            condition_name = item["conditionName"][0]
            match_type = item["matchType"]
            identifier_type = [
                detail["pptidentifierType"] for detail in file_details if detail["fileName"] == item["fileName"]
            ][0]
            if (
                corrective_action in CORRECTIVE_ACTIONS
                and match_type not in COMPARE_REPORT
                and (
                    (item["ifCondition"] == MET and item["actionStatus"] == "")
                    or (item["ifCondition"] == NOT_MET and item["actionStatus"] == "")
                )
            ):
                action_condition = self.action_for_condition(item, condition_name, corrective_action)

                if action_condition:
                    used_resp.append(item)
                    self.update_payload(
                        corrective_action,
                        action_condition,
                        payload_data,
                        identifier_type,
                        item=item,
                        ksdFileDetails=file_details,
                    )
            else:
                unused_resp.append(item)

        return (payload_data, used_resp, unused_resp)

    def call_tba_update(
        self,
        pjm_id: int,
        files: str,
        rule_engine_resp: List[dict],
        ksd_files_details: List[dict],
    ) -> List[dict]:
        """
        Call Tba update for the required fields

        Args:
            pjm_id (int): process job mapping id
            files (str): all files name separated with comma
            rule_engine_resp (List[dict]): rule engine failed responses
            ksd_files_details (List[dict]): all files with ksd details

        Returns:
            List[dict]: returns updated rule engine resposne data after TBA Update
        """

        payload_data, used_resp, unused_resp = self.tba_update_payload_data(rule_engine_resp, ksd_files_details)
        tba_update_config = self.get_complete_request()
        client_id = {"clientId": self.client_id}
        self.process_job_mapping.update(client_id)

        if "ksdName" in self.process_job_mapping.keys():
            del self.process_job_mapping["ksdName"]
        self.process_job_mapping["clientDetails"] = dict(self.process_job_mapping["clientDetails"])

        payload = {
            "processJobMapping": [dict(self.process_job_mapping)],
            "configTables": {"tbaUpdateConfig": tba_update_config},
            "rerun": payload_data["rerun"],
            "comment": payload_data["comment"],
            "requestData": payload_data["requestData"],
            "notice": payload_data["notice"],
        }

        response = None
        session = Session()
        try:
            headers = create_http_headers_for_new_span()
            headers["Content-Type"] = settings.CONTENT_TYPE
            LOGGER.info(f"Hitting TBA Update at URL: {settings.TBA_UPDATE_URL}", extra=self.header_details)
            response = session.post(
                url=settings.TBA_UPDATE_URL,
                data=json.dumps(payload),
                headers=headers,
            )

        except Exception as err:
            LOGGER.error(f"Unable to connect TBA Update {repr(err)}", extra=self.header_details)
            raise FileValidationError(self, "Unable to connect TBA Update", maestro="update_connect", name=files)

        if response and response.status_code == 200:
            LOGGER.info("Got Response from TBA Update", extra=self.header_details)
            update_response = response.json()
            if any(
                item in ("NewUpdate", "TBA_Rerun_response", "TBA_Notice_response", "TBA_pendingevents_response")
                for item in update_response.keys()
            ):
                unused_resp.extend(self.updated_fields(used_resp, update_response))
                return unused_resp
            else:
                LOGGER.error(
                    ERROR_MSG_UNABLE_GET_RESPONSE_TBAUPDATE + f" {response.content}", extra=self.header_details
                )
                raise FileValidationError(
                    self, ERROR_MSG_UNABLE_GET_RESPONSE_TBAUPDATE, maestro="update_resp1", name=files
                )

        else:
            LOGGER.error(ERROR_MSG_UNABLE_GET_RESPONSE_TBAUPDATE + f" {response.content}", extra=self.header_details)
            raise FileValidationError(self, ERROR_MSG_UNABLE_GET_RESPONSE_TBAUPDATE, maestro="update_resp2", name=files)

    def field_update(
        self, ppt_ssn: str, items: List[dict], event_name: str, status: str, val: Optional[str] = None
    ) -> None:
        """
        Update participant data after update, success if success otherwise reason

        Args:
            ppt_ssn (str): participant SSN
            items (List[dict]): rule engine used data
            event_name (str): Event name OR Update name
            status (str): success if success reaosn for fail otherwise
            val (Optional[str]): default value is None otherwise updated value

        Returns:
            None
        """

        for item in items:
            if item["participantSsn"] == ppt_ssn and item["eventName"] == event_name:
                if val and item["correctiveAction"][0] in (TBA_UPDATE,):
                    item.update({"tbaValue": val})
                if status.lower() == "success":
                    item.update({"actionStatus": status.capitalize()})
                    item.update({"reason": f"{item['correctiveAction'][0]} Success"})
                else:
                    item.update({"reason": f"{item['correctiveAction'][0]} Failed"})
                    item.update({"actionStatus": status})

    def new_update_response(self, update_req: List[dict], update_resp: dict) -> List[dict]:
        """
        Update TBA (Update, Validate, Delete & Add) data to audit response

        Args:
            update_req (List[dict]): ppt's data used for calling update for above actions
            update_resp (dict): resposne data for above actions

        Returns:
            List[dict]: Updated audit list of dictionaries
        """

        for resp in update_resp:
            identifier = resp["identifier"]
            if resp["status"].lower() == "success":
                for event_field, val in resp["fields"].items():
                    self.field_update(identifier, update_req, event_field, "Success", val)
            else:
                for event_field, val in resp["fields"].items():
                    self.field_update(identifier, update_req, event_field, resp["status"])

        return update_req

    def rerun_response(self, rerun_req: List[dict], rerun_resp: dict) -> List[dict]:
        """
        Update Rerun-Events data to audit response

        Args:
            rerun_req (List[dict]): ppt's data used for calling Update for rerun
            rerun_resp (dict): response data of rerun events

        Returns:
            List[dict]: Updated audit list of dictionaries
        """

        for resp in rerun_resp:
            identifier = resp["identifier"]
            event_name = resp["eventName"]
            action = RERUN_EVENT if resp["action"] == "Rerun" else RERUN_EVENT_DELETE
            for req in rerun_req:
                if (
                    req["participantSsn"] == identifier
                    and req["rerunEvent"] == event_name
                    and req["correctiveAction"][0] == action
                ):
                    if resp["reason"].lower() == "success":
                        req.update({"actionStatus": resp["reason"].capitalize()})
                        req.update({"reason": f"{req['correctiveAction'][0]} Success"})
                    else:
                        req.update({"actionStatus": resp["reason"]})
                        req.update({"reason": f"{req['correctiveAction'][0]} Failed"})

        return rerun_req

    def update_notice_fields(self, resp, req, temp):

        if resp["reason"].lower() == "success":
            req.update({"actionStatus": resp["reason"].capitalize()})
            req.update({"reason": f"{req['correctiveAction'][0]} Success"})
        else:
            req.update({"actionStatus": resp["reason"]})
            req.update({"reason": f"{req['correctiveAction'][0]} Failed"})

    def notice_response(self, notice_req: List[dict], notice_resp: dict) -> List[dict]:
        """
        Update Notice Cancel data to audit response

        Args:
            notice_req (List[dict]): ppt's data used for calling Update for notice cancel
            notice_resp (dict): response data of notice cancel

        Returns:
            List[dict]: Udpated audit list of dictionaries
        """

        for resp in notice_resp:
            identifier = resp["participantId"]
            inq_def_name = resp["inquiryDefName"]

            for req in notice_req:
                if req["participantSsn"] == identifier:
                    if req["correctiveAction"][0] in (TBA_NOTICE_CANCEL,) and inq_def_name == req["noticeUpdate"]:
                        self.update_notice_fields(resp, req, False)
                    elif req["correctiveAction"][0] in (TBA_NOTICE_UPDATE,) and inq_def_name in req["noticeCancel"]:
                        self.update_notice_fields(resp, req, True)

        return notice_req

    def pending_response(self, pend_req: List[dict], pend_resp: dict) -> List[dict]:
        """
        Update pending events data to audit response

        Args:
            pend_req (List[dict]): ppt's data used for calling update for pending events
            pend_resp (dict): response data of pending events

        Returns:
            List[dict]: Updated audit list of dictionaries
        """
        for resp in pend_resp:
            identifier = resp["identifier"]
            inq_def_name = resp["inquiryDefName"]

            for req in pend_req:
                if req["identifier"] == identifier:
                    if req["correctiveAction"][0] in (TBA_PENDEVNT_UPDATE,) and inq_def_name == req["pendingEventName"]:
                        self.update_notice_fields(resp, req, False)
                    elif (
                        req["correctiveAction"][0] in (TBA_PENDEVNT_CANCEL,) and inq_def_name == req["pendingEventName"]
                    ):
                        self.update_notice_fields(resp, req, True)

    def updated_fields(self, used_resp: List[dict], udpate_response: dict) -> List[dict]:
        """
        Update audit json response with tba update resposne

        Args:
            used_resp (List[dict]): used rule response for calling tba update
            update_response (dict): response which we got from tba update

        Retrns:
            List[dict]: updated rule engine response
        """
        new_resp = list()
        resp_keys = udpate_response.keys()

        update_req = [
            update
            for update in used_resp
            if update["correctiveAction"][0]
            in (
                TBA_UPDATE,
                "TBA Add",
                "TBA Delete",
                "TBA Validate",
            )
        ]
        rerun_req = [rerun for rerun in used_resp if rerun["correctiveAction"][0] in (RERUN_EVENT,)]
        notice_req = [
            notice for notice in used_resp if notice["correctiveAction"][0] in (TBA_NOTICE_CANCEL, TBA_NOTICE_UPDATE)
        ]
        pending_req = [
            pend for pend in used_resp if pend["correctiveAction"][0] in (TBA_PENDEVNT_CANCEL, TBA_PENDEVNT_UPDATE)
        ]

        if "NewUpdate" in resp_keys:
            new_resp.extend(self.new_update_response(update_req, udpate_response["NewUpdate"]))
        if "TBA_Rerun_response" in resp_keys:
            new_resp.extend(self.rerun_response(rerun_req, udpate_response["TBA_Rerun_response"]))
        if "TBA_Notice_response" in resp_keys:
            new_resp.extend(self.notice_response(notice_req, udpate_response["TBA_Notice_response"]))
        if "TBA_pendingevents_response" in resp_keys:
            new_resp.extend(self.pending_response(pending_req, udpate_response["TBA_pendingevents_response"]))

        return new_resp

    def call_excel_formatter(self, files: str, oprep={}, opfile={}) -> dict:
        """
        Call excel formatter and get output report frame

        Args:
            files (str): Name of the file(s)
            oprep (dict): output report keys
            opfile (dict): output file keys
        Returns:
            dict: return botOutput dictionary
        """

        session = Session()
        payload = {
            "ksdConfig": self.ksd_config,
            "botOutput": self.bot_output,
            "processFeatureConfig": self.process_feature_config,
            "ksdOutputFileDetails": self.ksd_output_file_details,
            "layoutConfig": self.layout_config,
        }
        payload.update({"outputReports": oprep})
        payload.update({"outputFiles": opfile})

        response = None
        try:
            headers = create_http_headers_for_new_span()
            headers["Content-Type"] = settings.CONTENT_TYPE
            LOGGER.info(f"Hitting Excel Formatter at URL: {settings.EXCEL_FORMATTER_URL}", extra=self.header_details)
            response = session.post(url=settings.EXCEL_FORMATTER_URL, data=json.dumps(payload), headers=headers)

        except Exception as err:
            LOGGER.error(f"Unable to connect Excel Formatter {repr(err)}", extra=self.header_details)
            raise FileValidationError(self, "Unable to connect Excel Formatter", maestro="excel_connect", name=files)

        if response is not None and response.status_code == 200:
            response_json = response.json()

            if response_json["status"].lower() == "success":
                return response_json["botOutput"]

        LOGGER.error(f"Failed response from Excel Formatter {response.content}", extra=self.header_details)
        raise FileValidationError(self, "Failed response from Excel Formatter", maestro="excel_connect", name=files)

    def update_whole_column(self, cells: List[tuple], r_df: pd.DataFrame, ssn: str, output_col: dict) -> pd.DataFrame:
        """
        Update One column for all participants

        Args:
            file_detail (dict): dictionary with one output file detail
            file_details (List[dict]): List of all output file details
            ssn (str): pptidentifier field in r_df
        Returns:
            pd.DataFrame: updated frame
        """
        redis_df = r_df

        for index, row in r_df.iterrows():
            update_value = list()
            for cell in cells:
                update_value.append(
                    self.get_field_value("tba", cell[1], cell[2], cell[3], self.ksdfiles_details, row[ssn])
                )

                redis_df.at[index, output_col["dataElementWoutSpace"]] = ", ".join(update_value)

        return redis_df

    def populate_tba_values(self, redis_df: pd.DataFrame, detail: dict) -> pd.DataFrame:
        """
        Populate TBA values for 'Value From Source' is TBA

        Args:
            redis_df (pd.DataFrame): output redis frame
            detail (dict): output file details
        Returns:
            pd.DataFrame: updated frame with tba values
        """

        for output_col in detail["outputReports"]:
            cell_value = output_col["cellValue"].split(",")
            if len(cell_value) > 1 and "value from source" in cell_value[0].lower():
                cells = [
                    tuple(cell_value[index : index + 5])
                    for index in range(1, len(cell_value), 4)
                    if "tba" in cell_value[index].lower()
                ]
                redis_df = self.update_whole_column(cells, redis_df, detail["ssn"], output_col)

        return redis_df

    def get_output_ksdfile_details(self, output_reports: dict) -> List[dict]:
        """
        Extract required details from ksdOutputFileDetails

        Args:
            output_reports (dict): output reports given by excel formatter
        """

        files_details = list()
        botoutput = output_reports["outputReports"]
        for ksdfile in self.ksd_output_file_details:
            _detail = dict()
            _detail["ssn"] = self.get_pptidentifier(
                ksdfile["fileName"],
                ksdfile["pptIdentifier"],
                ksdfile["outputReports"],
                {"fieldName": "dataElement", "fieldNameWoutSpace": "dataElementWoutSpace"},
            )
            _detail["fileName"] = ksdfile["fileName"]
            _detail["fileNameWoutSpace"] = ksdfile["fileNameWoutSpace"]
            _detail["sheetName"] = ksdfile["sheetNameWoutSpace"]
            _detail["fileType"] = ksdfile["fileType"]
            _detail["pptIdentifierType"] = ksdfile["pptIdentifierType"]
            _detail["outputReports"] = ksdfile["outputReports"]
            redis_keys = botoutput[_detail["fileName"]]

            for key in redis_keys:
                file_name = _detail["fileName"]
                LOGGER.info(f"Fetching redis key: {key} for file: {file_name}", extra=self.header_details)
                ppt_identifier = _detail["ssn"]
                detail = copy.deepcopy(_detail)
                detail.update({"identifierName": key["identifier_name"]})
                detail.update({"sheetNameWoutSpace": key["sheet_name"]})
                detail.update({"detailRedisKey": key["key"]})
                detail.update({"sheetNameWoutSpace": key["sheet_name"]})
                redis_frame = self.fetch_file_redis(key["key"], file_name)
                redis_frame = redis_frame[pd.notnull(redis_frame[ppt_identifier])]
                redis_frame = redis_frame[redis_frame[ppt_identifier].apply(lambda x: x.strip()) != ""]
                redis_frame = self.populate_tba_values(redis_frame, _detail)
                detail.update({"required_frame": redis_frame})
                files_details.append(detail)

        LOGGER.info("Extracted ksdFileDetails from ksdOutputFileDetails", extra=self.header_details)
        return files_details

    def call_file_update(self, files: str, rule_engine_resp: List[dict]) -> List[dict]:
        """
        Perform steps for file update

        Args:
            files (str): Name of the file(s)
            rule_engine_resp List[dict]: response from rule engine
        Returns:
            List[dict]: returns updated rule engine response data after file update
        """

        output_reports = self.call_excel_formatter(files)
        ksd_outfiles_details = self.get_output_ksdfile_details(output_reports)

        for resp in rule_engine_resp:
            actions = resp["updateAction"]
            ppt = resp["participantSsn"]
            result_var = resp["resultsVarable"]
            if resp["correctiveAction"][0] == FILE_REPORT_UPDATE and resp["actionStatus"] not in (NO_ACTION_IS_TAKEN,):
                action_stat = list()
                for action in actions:
                    file_update_data: dict = self.get_output_file_data(action, ppt, result_var, ksd_outfiles_details)
                    action_status, status = self.update_output_frame(ksd_outfiles_details, file_update_data)
                    action_stat.append((action_status, status))
                whole_stat = [stat[0] for stat in action_stat if "failed" in stat[0].lower()]
                resp["actionStatus"], resp["reason"] = (
                    (", ".join(whole_stat), f"{FILE_REPORT_UPDATE} Failed")
                    if len(whole_stat) > 0
                    else ("Success", f"{FILE_REPORT_UPDATE} Success")
                )

        for setter in ksd_outfiles_details:
            frame_filename = setter["fileName"]
            pkl_frame_filename = combined_name(frame_filename, setter["identifierName"], setter["sheetNameWoutSpace"])
            frame = setter["required_frame"]
            frame.to_pickle(pkl_frame_filename, compression="zip")
            with open(pkl_frame_filename, "rb") as f:
                data = {"file": f}
                response = self.set_file_redis(frame_filename, data)
            os.remove(pkl_frame_filename)
            if response["status"] == "success":
                load = [
                    {
                        "sheet_name": setter["sheetNameWoutSpace"],
                        "key": pkl_frame_filename,
                        "identifier_name": setter["identifierName"],
                    }
                ]
                oprep = {frame_filename: load}
                second_bo = self.call_excel_formatter(files, oprep, output_reports["outputFiles"])
                self.excel_botoutput = second_bo["outputFiles"][frame_filename]
                self.redis_keys.update({self.excel_botoutput: self.excel_botoutput})

        return rule_engine_resp

    def update_output_frame(self, file_details: List[dict], data: dict) -> Tuple[str, str]:
        """
        Update output data frame

        Args:
            file_details (List[dict]): ksdoutputfiledetails data
            data (dict): data which needs to be updated
        Return:
            str: actionStatus string to update audit json
            str: reason for audit json update
        """

        for file_detail in file_details:
            if (
                file_detail["fileName"] == data["fromFileName"]
                and file_detail["identifierName"] == data["fromFileIdentifier"]
                and file_detail["sheetName"] == data["fromFileSheetName"]
            ):
                if data["fromFileField"] not in file_detail["required_frame"].columns.tolist():
                    LOGGER.warning(
                        f"{data['fromFileField']} not found {data['fromFileName']}", extra=self.header_details
                    )
                    return (f"{data['fromFileField']} not found {data['fromFileName']}", f"{FILE_REPORT_UPDATE} Failed")
                try:
                    redis_frame = file_detail["required_frame"]
                    redis_frame.loc[redis_frame[file_detail["ssn"]] == data["pptId"], data["fromFileField"]] = data[
                        "fieldValue"
                    ]
                    return ("Success", f"{FILE_REPORT_UPDATE} Success")
                except Exception as err:
                    LOGGER.error(f"{FILE_REPORT_UPDATE} Failed for {repr(err)}", extra=self.header_details)
                    return ("Failed", f"{FILE_REPORT_UPDATE} Failed")
                break
        else:
            LOGGER.error(
                f"file name configured in corrective action not matched with ksdoutputfiledetails",
                extra=self.header_details,
            )
            return ("Failed for file name mismatch", f"{FILE_REPORT_UPDATE} Failed")

    def get_output_file_data(self, action: dict, ppt_id: str, result_var: list, file_details: List[dict]) -> dict:
        """
        Process action for file report update and get the required data

        Args:
            action (dict): action to be performed for file/report udpate
            ppt_id (str): participant id for which update is to be performed
            result_var (list): result variable fields if required
            file_details (List[dict]): ksdfile details for output file
        Returns:
            dict: dictionary with all required data to update output frame
        """

        data = {
            "fromFileName": action["fromFileName"],
            "fromFileSheetName": action["fromFileSheetName"],
            "fromFileIdentifier": action["fromFileIdentifier"],
            "fromFileField": action["fromFileField"],
            "pptId": ppt_id,
        }
        field_value = None

        if action["updateToRadio"] == "text":
            field_value = action["updateToText"]

        elif action["updateToRadio"] == "date":
            field_value = dateproperformat(action["updateToDate"], "CCYY-MM-DD")

        elif action["updateToRadio"] == "field":
            updt_file = action["updateToFileName"]
            updt_sheet = action["updateToSheetName"]
            updt_identifier = action["updateToFileIdentifier"]
            updt_field = action["updateToFileField"]

            # If not found field_value will be 'Error: Not Found'
            field_value = self.get_field_value(
                updt_file, updt_sheet, updt_identifier, updt_field, self.ksdfiles_details + file_details, ppt_id
            )

        elif action["updateToRadio"] == "resultVar":
            temp_value = [
                field[action["updateToResult"]] for field in result_var if action["updateToResult"] in field.keys()
            ]
            if len(temp_value) > 0:
                field_value = temp_value[0]
            else:
                field_value = "Erorr: Value Not Found"

        data.update({"fieldValue": field_value})

        return data

    def isupdate(self, rule_engine_resp: list, actions: list, ft_flag="tba") -> bool:
        """
        Check if tba OR file Update needs to be called

        Args:
            rule_engine_resp (List[dict]): full rule engine response
            actions (list): tba actions or file actions
        Returns:
            bool: true if any corrective action is present in TBA update corrective action
        """

        LOGGER.info(f"Checking {actions} Update", extra=self.header_details)
        for resp in rule_engine_resp:
            corrective_action = ""
            match_type = resp["matchType"]
            if len(resp["correctiveAction"]) > 0:
                corrective_action = resp["correctiveAction"][0]

            # In case of 'file' check ksdOutputFileDetails first
            if ft_flag == "file" and len(self.ksd_output_file_details) > 0:
                return True

            if (
                corrective_action in actions
                and match_type not in COMPARE_REPORT
                and (
                    (resp["ifCondition"] == NOT_MET and resp["actionStatus"] == "")
                    or (resp["ifCondition"] == MET and resp["actionStatus"] == "")
                )
            ):
                return True
        return False

    def is_human_in_loop(self, response: List[dict]) -> bool:
        """
        Check If needs to raise human in loop

        Args:
            response (List[dict]): response of rule engine/ tba udpate

        Returns:
            bool: true if needs to raise HIL, false otherwise
        """
        hil_flag = False

        for resp in response:
            resp.pop("id", None)
            resp.pop("rerunEvent", None)
            resp.pop("noticeCancel", None)
            resp.pop("noticeUpdate", None)
            resp.pop("resultsVarable", None)
            resp.pop("updateAction", None)
            resp.pop("pendingEventName", None)
            resp.pop("matchType", None)
            resp.update({"eventName": self.update_event_name.get(resp["eventName"], "")})

            if resp["actionStatus"].lower() not in (
                "success",
                NO_ACTION_IS_TAKEN,
            ):
                hil_flag = True
            if resp["actionStatus"].lower() in (NO_ACTION_IS_TAKEN,) and any(
                item.lower() == "human in loop" for item in resp["correctiveAction"]
            ):
                resp.update({"actionStatus": ""})
                resp.update({"reason": "No action is required"})

        return hil_flag

    def is_not_in_tba(self, ksdfiles_details: List[dict], inq_resp: list) -> Tuple[bool, list]:
        """
        Process inquiry response and check if participant not in TBA

        Args:
            ksdfiles_details (List[dict]): list of ksd file details
            inq_resp (list): list of response from inquiry

        Returns:
            Tuple[bool, list]
        """

        audit_resp = list()
        not_in_tba = True
        for index, ksdfile in enumerate(ksdfiles_details):
            mismatch_data = list()
            LOGGER.info("Started Fetching Inquiry Details", extra=self.header_details)
            failed_ppt = sorted(inq_resp[index]["inquiry_response"][1], reverse=True, key=lambda x: x["index"])

            for participant in failed_ppt:
                participants = dict()
                participants["uid"] = self.uid
                participants["participantSsn"] = participant[str(ksdfile["pptidentifierType"]).lower()]
                participants["internalId"] = ""
                participants["participantName"] = ""
                participants["fileName"] = ksdfile["fileName"]
                participants["sheetName"] = ksdfile["sheetName"]
                participants["dataMismatch"] = ""
                participants["tbaFieldName"] = ""
                participants["mainframeValue"] = ""
                participants["tbaValue"] = ""
                participants["ruleName"] = ""
                participants["ruleFailedOnField"] = []
                participants["correctiveAction"] = [HUMAN_IN_LOOP]
                participants["conditionName"] = []
                participants["ifCondition"] = ""
                participants["reason"] = participant.get("errorDescription", "Participant Not Found")
                participants["eventName"] = ""
                participants["effectiveDate"] = ""
                participants["actionStatus"] = ""
                mismatch_data.append(participants)

                inq_resp[index]["participant_list"].pop(participant["index"])

            audit_resp += mismatch_data

            if len(inq_resp[index]["participant_list"]) > 0:
                not_in_tba = False
                if inq_resp[index]["inquiry_response"][0]:
                    ksdfile.update(
                        {
                            "tba_frame": self.get_response_dict(
                                inq_resp[index]["participant_list"],
                                inq_resp[index]["inquiry_response"][0],
                            )
                        }
                    )
                ksdfile.update({"ppt_list": inq_resp[index]["participant_list"]})
            else:
                ksdfile.update({"ppt_list": []})

        return (not_in_tba, audit_resp)

    def get_response_dict(self, participant_list: list, inq_resp: list):
        """
        extracting and segregating inquired data based on participant id.

        Args :
            participant_list : list of participant id (eid/pid/iid)
            inq_resp         : list of inquired data.

        """
        temp = dict()
        for index, participant in enumerate(participant_list):
            participant_data = inq_resp[index]
            temp[participant] = dict()
            for item in participant_data:
                field_list = list()
                data = participant_data[item]
                if isinstance(data, list):
                    for x in data:
                        x_keys = x.keys()
                        field_list.extend(x_keys)
                        self.pick_internal_id(participant, x_keys, x)
                elif isinstance(data, str):
                    field_list.append(item)
                    data = [{item: data}]

                temp[participant].update({tuple(set(field_list)): data})
        return temp

    def get_response(self) -> dict:
        """get_response will call required functions to complete the task"""

        # filter inquiry and notices fields with inquiry_lookup check.
        self.check_match_config()
        self.tba_inquiry_config = self.filtered_fields(self.inquiry_config, "inquiryDefName")
        self.tba_notice_inq_config = self.filtered_fields(self.notice_config, "inquiryDefName")
        self.check_file_identifier()
        if self.errors:
            msg = ", ".join(self.errors)
            LOGGER.error(msg, extra=self.header_details)

            raise FileValidationError(self, msg, maestro="not_valid")

        if len(self.tba_match_config) < 1:
            audit_response = {
                "status": "Success",
                "statusMessage": "No Configuration(s) found in Match TBA/Report",
                "overAllStatus": True,
                "fileName": "",
                "sheetName": "",
                "participants": self.ppt_total,
                "participantsVerified": self.ppt_verified,
                "participantsSuccess": self.ppt_success,
                "participantsFailed": self.ppt_failed,
            }
            self.audit.update({"fileName": ""})
            self.audit.update({"fileType": ""})
            self.audit.update({"createTimestamp": self.create_time_stamp})
            self.audit.update({"json": json.dumps(audit_response)})
            return {
                "audit": self.audit,
                "maestro": {},
                "data": {},
                "botOutput": {
                    "participants": self.ppt_total,
                    "participantsVerified": self.ppt_verified,
                    "participantsSuccess": self.ppt_success,
                    "participantsFailed": self.ppt_failed,
                },
                "status": "Success",
                "statusMessage": "",
                "noConfigStatusMessage": "No Configuration(s) found in Match TBA/Report",
                "overAllStatus": True,
                "processLog": [
                    {
                        "uid": self.uid,
                        "processJobMappingId": self.pjm_id,
                        "botId": "MFvsTba",
                        "elementType": "status",
                        "value": "Success",
                        "timestamp": str(datetime.now().replace(microsecond=0).isoformat()),
                    }
                ],
                "redisKeys": self.redis_keys,
            }

        ksdfiles_details, files, files_type, sheets = self.get_ksdfiles_details()
        self.ksdfiles_details = ksdfiles_details

        if len(ksdfiles_details) < 1:
            LOGGER.info("None of the identifier have match fields", extra=self.header_details)
            raise FileValidationError(
                self, "None of the identifier have match fields", maestro="identifier_mismatch", name=",".join(files)
            )

        response_from_inquiry = list()
        files = list(files)
        sheets = list(sheets)
        files_type = list(files_type)

        for ksdfile in ksdfiles_details:

            LOGGER.info(f"Hitting TBA Inquiry for identifier: ({ksdfile['identifierName']})", extra=self.header_details)

            participant_list, inquiry_payload = self.get_tba_inquiry_payload(
                file_name=ksdfile["fileName"],
                file_type=ksdfile["fileType"],
                redis_frame=ksdfile["required_frame"],
                identifier_name=ksdfile["identifierName"],
                redis_pid_name=ksdfile["ssn"],
                identifier_type=ksdfile["pptidentifierType"],
            )
            inquiry_response_details = dict()
            inquiry_response_details["participant_list"] = participant_list
            inquiry_response_details["inquiry_response"] = self.call_tba_inquiry(
                inquiry_data=inquiry_payload, files=",".join(files)
            )
            response_from_inquiry.append(inquiry_response_details)
            LOGGER.info(
                f"Got response from TBA Inquiry for identifier: ({ksdfile['identifierName']})",
                extra=self.header_details,
            )

        participant_not_in_tba_flag, audit_resp = self.is_not_in_tba(ksdfiles_details, response_from_inquiry)

        if participant_not_in_tba_flag:
            LOGGER.error(ERROR_MSG_PARTICIPANT_NOT_TBA, extra=self.header_details)
            audit_response = json.dumps(
                {
                    "MFvsTba": mask_ssn(audit_resp),
                    "status": HUMAN_IN_LOOP,
                    "statusMessage": ERROR_MSG_PARTICIPANT_NOT_TBA,
                    "overAllStatus": False,
                    "fileName": ",".join(files),
                    "sheetName": ",".join(sheets),
                    "participants": self.ppt_total,
                    "participantsVerified": 0,
                    "participantsSuccess": 0,
                    "participantsFailed": 0,
                }
            )

            match_response = {
                "uid": self.uid,
                "processJobMappingId": self.pjm_id,
                "botName": self.plugin_name,
                "ticketId": 0,
                "fileName": ",".join(files),
                "fileType": ",".join(files_type),
                "clientDet": self.client_id,
                "allocatedBy": self.user_name,
                "createTimestamp": self.create_time_stamp,
                "flag": True,
                "json": audit_response,
            }
            response = {
                "audit": match_response,
                "maestro": {
                    "description": "Participants not in TBA",
                    "title": ERROR_MSG_PARTICIPANT_NOT_TBA,
                },
                "botOutput": {
                    "participants": self.ppt_total,
                    "participantsVerified": 0,
                    "participantsSuccess": 0,
                    "participantsFailed": 0,
                },
                "status": HUMAN_IN_LOOP,
                "statusMessage": ERROR_MSG_PARTICIPANT_NOT_TBA,
                "noConfigStatusMessage": "",
                "overAllStatus": False,
                "processLog": [
                    {
                        "uid": self.uid,
                        "processJobMappingId": self.pjm_id,
                        "botId": "MFvsTba",
                        "elementType": "status",
                        "value": HUMAN_IN_LOOP,
                        "timestamp": str(datetime.now().replace(microsecond=0).isoformat()),
                    }
                ],
                "redisKeys": self.redis_keys,
            }

            return response

        rule_audit_resp, rule_success_resp = self.call_rule_engine(ksdfiles_details, ",".join(files))

        full_rule_resp = list()
        full_rule_resp.extend(rule_audit_resp)
        full_rule_resp.extend(rule_success_resp)
        self.add_internal_id(full_rule_resp)

        # check for file Update
        if self.isupdate(full_rule_resp, [FILE_REPORT_UPDATE], ft_flag="file"):
            full_rule_resp = self.call_file_update(files, full_rule_resp)

        # check for tba Update
        if self.isupdate(full_rule_resp, CORRECTIVE_ACTIONS):
            full_rule_resp = self.call_tba_update(
                pjm_id=self.pjm_id,
                files=",".join(files),
                rule_engine_resp=full_rule_resp,
                ksd_files_details=ksdfiles_details,
            )

        full_rule_resp.extend(audit_resp)
        LOGGER.info(f"Not_Found_ppt length: {len(audit_resp)}", extra=self.header_details)

        # Get set of failed ppt and calculate success ppt from verified
        failed_count = {
            failed["participantSsn"]
            for failed in full_rule_resp
            if failed["actionStatus"].lower() not in ("success", NO_ACTION_IS_TAKEN)
        }
        self.ppt_failed = len(failed_count)
        self.ppt_success = self.ppt_verified - self.ppt_failed
        LOGGER.info(
            f"Participants: {self.ppt_verified}\n Success: {self.ppt_success}\n Failed: {self.ppt_failed}",
            extra=self.header_details,
        )

        if self.is_human_in_loop(full_rule_resp):
            sorted_full_resp = sorted(full_rule_resp, key=itemgetter("participantSsn", "dataMismatch"))
            LOGGER.info(f"Human In Loop Response for UID: {self.uid}", extra=self.header_details)

            audit_response = json.dumps(
                {
                    "MFvsTba": mask_ssn(sorted_full_resp),
                    "status": HUMAN_IN_LOOP,
                    "statusMessage": "Mismatch data found",
                    "overAllStatus": False,
                    "fileName": ",".join(files),
                    "sheetName": ",".join(sheets),
                    "participants": self.ppt_total,
                    "participantsVerified": self.ppt_verified,
                    "participantsSuccess": self.ppt_success,
                    "participantsFailed": self.ppt_failed,
                }
            )
            self.audit.update({"fileName": ",".join(files)})
            self.audit.update({"fileType": ",".join(files_type)})
            self.audit.update({"createTimestamp": self.create_time_stamp})
            self.audit.update({"json": audit_response})

            maestro_payload = {
                "description": "File/Report vs TBA mismatched data found",
                "title": "Mismatched data",
            }

            bot_out_put = {
                "participants": self.ppt_total,
                "participantsVerified": self.ppt_verified,
                "participantsSuccess": self.ppt_success,
                "participantsFailed": self.ppt_failed,
            }
            if self.excel_botoutput != "":
                bot_out_put.update({self.excel_botoutput: self.excel_botoutput})
            return {
                "audit": self.audit,
                "maestro": maestro_payload,
                "data": {},
                "botOutput": bot_out_put,
                "status": HUMAN_IN_LOOP,
                "statusMessage": "Mismatched data found",
                "noConfigStatusMessage": "",
                "overAllStatus": False,
                "processLog": [
                    {
                        "uid": self.uid,
                        "processJobMappingId": self.pjm_id,
                        "botId": "MFvsTba",
                        "elementType": "status",
                        "value": HUMAN_IN_LOOP,
                        "timestamp": str(datetime.now().replace(microsecond=0).isoformat()),
                    }
                ],
                "redisKeys": self.redis_keys,
            }
        else:
            LOGGER.info(f"Success Response for UID: {self.uid}", extra=self.header_details)
            sorted_full_resp = sorted(full_rule_resp, key=itemgetter("participantSsn", "dataMismatch"))

            audit_response = json.dumps(
                {
                    "MFvsTba": mask_ssn(sorted_full_resp),
                    "status": "Success",
                    "statusMessage": NO_MISMATCH,
                    "overAllStatus": True,
                    "fileName": ",".join(files),
                    "sheetName": ",".join(sheets),
                    "participants": self.ppt_total,
                    "participantsVerified": self.ppt_verified,
                    "participantsSuccess": self.ppt_success,
                    "participantsFailed": self.ppt_failed,
                }
            )
            self.audit.update({"fileName": ",".join(files)})
            self.audit.update({"fileType": ",".join(files_type)})
            self.audit.update({"createTimestamp": self.create_time_stamp})
            self.audit.update({"json": audit_response})
            bot_out_put = {
                "participants": self.ppt_total,
                "participantsVerified": self.ppt_verified,
                "participantsSuccess": self.ppt_success,
                "participantsFailed": self.ppt_failed,
            }
            if self.excel_botoutput != "":
                bot_out_put.update({self.excel_botoutput: self.excel_botoutput})
            return {
                "audit": self.audit,
                "maestro": {},
                "data": {},
                "botOutput": bot_out_put,
                "status": "Success",
                "statusMessage": NO_MISMATCH,
                "noConfigStatusMessage": "",
                "overAllStatus": True,
                "processLog": [
                    {
                        "uid": self.uid,
                        "processJobMappingId": self.pjm_id,
                        "botId": "MFvsTba",
                        "elementType": "status",
                        "value": "Success",
                        "timestamp": str(datetime.now().replace(microsecond=0).isoformat()),
                    }
                ],
                "redisKeys": self.redis_keys,
            }
