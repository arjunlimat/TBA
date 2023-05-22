"""testcase"""
from copy import deepcopy
from unittest import mock

from requests import Session
from django.test import TestCase, Client, override_settings
from rest_framework.response import Response


content_type = "application/json"
payload = {
    "ksdConfig": {
        "processJobMapping": {
            "eftSubject": "TEMP",
            "jobName": "TEMP",
            "businessUnitOps": {},
            "clientDetails": {
                "businessUnitClients": [],
                "createdDate": "2020-04-03T09:18:05.607+0000",
                "clientName": "Temp Client",
                "createdBy": "ADMIN",
                "clientCode": "12345",
                "id": 1,
            },
            "process": {},
            "createdDate": "2020-04-23T12:37:31.364+0000",
            "createdBy": "ADMIN",
            "ksdName": "TMEP",
            "id": 101,
        },
        "ksdFileDetails": [
            '{"dateFormat":"YYYYMMDD","delimiter":"","fileFormatType":"Position","fileName":"TEMP.MAINFRAME.FILE","fileNameWoutSpace":"tempMainframeFile","fileType":"Mainframe","id":57,"pptidentifier":"SSN","pptidentifierType":"PID","processJobMappingId":101,"sheetName":"","sheetNameWoutSpace":"","subj":null}'
        ],
    },
    "botOutput": {
        "File Formatter": {
            "TEMP.MAINFRAME.FILE": {
                "detailRedisKey": [
                    {"identifier_name": "", "key": "TEMP.MAINFRAME.FILE_detail_data.pkl", "sheet_name": ""}
                ],
            }
        },
        "File Validator": {
            "TEMP.MAINFRAME.FILE": {
                "detailRedisKey": [
                    {"identifier_name": "", "key": "TEMP.MAINFRAME.FILE__filtered.pkl", "sheet_name": ""}
                ]
            }
        },
    },
    "requestDetails": {
        "uid": "RQ-1013080005041077",
        "userName": "clark",
        "pluginName": "Source Match",
        "phase": 4,
        "createTimeStamp": 1611936788504,
    },
    "processFeatureConfig": {
        "businessUnitName": "HWS",
        "phaseNames": '{"SourceMatch":"TEMP.MAINFRAME.FILE"}',
        "processType": "INBOUND",
        "businessOpsName": "Files & Interfaces",
        "processName": "TEMP",
        "processJobMapping": {
            "id": 123,
            "jobName": "TEMP",
        },
    },
    "configTables": {
        "tbaUpdateConfig": [],
        "rulesConfig": [],
        "tbaMatchConfig": [
            {
                "id": 1,
                "matchType": "Compare with TBA",
                "fileName": "TEMP.MAINFRAME.FILE",
                "sheetName": "",
                "fileNameWoutSpace": "tempMainframeFile",
                "sheetNameWoutSpace": "",
                "mfFieldName": "Temp Field",
                "mfFieldWoutSpace": "tempField",
                "identifier": "",
                "tbaFieldName": "temp TBA Field",
                "inquiryDefName": "TEMP_TBA_FIELD",
                "ruleName": "",
                "actions": '[{"condition": "", "statisfied":"Not Met", "correctiveAction": "Human In Loop", "reason": "NA", "actions":[]}]',
                "pptVerifyTba": "NA",
            },
        ],
        "tbaInquiryConfig": [
            {
                "id": 11,
                "inquiryName": "Temp Data",
                "parNM": "AB1234",
                "panelId": 1234,
                "tbaFieldName": "Temp TBA Field",
                "fieldType": "string",
                "jsonKey": "temp",
                "subJsonKey": "",
                "metaData": "",
                "identifier": "",
                "recordIdentifier": "",
                "inquiryDefName": "TEMP_TBA_FIELD",
                "sequence": "1",
                "effDateType": "date",
                "effFromDate": '{"effectiveFromDateAppNameWithoutSpace":"","effectiveFromDateSheetName":"","effectiveFromDateRIdentifier":"","effectiveFromDateField":"","effectiveFromDatePeriod":"Current","effectiveFromDateInterval":"Date"}',
                "effToDate": '{"effectiveToDateAppNameWithoutSpace":"","effectiveToDateSheetName":"","effectiveToDateRIdentifier":"","effectiveToDateField":"","effectiveToDatePeriod":"","effectiveToDateFrequency":"","effectiveToDateInterval":""}',
                "rowMatrix": "",
                "columnMatrix": "",
            }
        ],
        "tbaNoticeInqConfig": [],
        "tbaEventHistInqConfig": [],
        "tbaPendingEventInqConfig": [],
        "ksdOutputFileDetails": [],
        "layoutConfig": [
            {
                "id": 1,
                "fileName": "TEMP.MAINFRAME.FILE",
                "recordType": "Detail Record",
                "mfFieldName": "SSN",
                "recordFormat": "X(01)",
                "fieldType": "Text",
                "mfFieldWoutSpace": "ssn",
                "fileNameWoutSpace": "tempMainframeFile",
            }
        ],
    },
    "redisKeys": {},
}


class TestEmptyConfigs(TestCase):
    def setUp(self):
        self.url = "/sourceMatcher/fileVerification/"
        self.client = Client()
        self.payload = deepcopy(payload)

    def test_bad_request(self):
        response = self.client.post(self.url, data={}, content_type=content_type)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["statusMessage"], "Some important fields are missing")

    def test_inquiry_field_not_configured(self):
        """test inquiry field configured for matching but not configured in inquiry config(s)"""
        self.payload["configTables"]["tbaMatchConfig"][0].update(
            {
                "inquiryDefName": "SOME_DEF_NAME"
            }
        )
        response = self.client.post(self.url, data=self.payload, content_type=content_type)
        data = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["status"], "Failed")
        self.assertEqual(data["statusMessage"], "SOME_DEF_NAME is not configured in TBA")

    def test_inquiry_field_with_bad_identifier(self):
        """test inquiry field configured with incorrect identifier"""
        self.payload["configTables"]["tbaInquiryConfig"][0].update(
            {
                "identifier": "temp"
            }
        )
        response = self.client.post(self.url, data=self.payload, content_type=content_type)
        data = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["status"], "Failed")
        self.assertEqual(data["statusMessage"], "TEMP_TBA_FIELD not configured with identifier '' in TBA")

    def test_empty_match_config(self):
        self.payload["configTables"].update(
            {
                "tbaMatchConfig": [],
            }
        )
        response = self.client.post(self.url, data=self.payload, content_type=content_type)
        data = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["status"], "Success")
        self.assertEqual(data["noConfigStatusMessage"], "No Configuration(s) found in Match TBA/Report")

    def test_empty_ksd_file_details(self):
        """match config with irrelevant input files/identifier"""
        self.payload["ksdConfig"].update(
            {
                "ksdFileDetails": [
                    '{"dateFormat":"YYYYMMDD","delimiter":"","fileFormatType":"Position","fileName":"SOME.FILE","fileNameWoutSpace":"someFile","fileType":"Mainframe","id":57,"pptidentifier":"SSN","pptidentifierType":"PID","processJobMappingId":101,"sheetName":"","sheetNameWoutSpace":"","subj":null}'
                ]
            }
        )
        response = self.client.post(self.url, data=self.payload, content_type=content_type)
        data = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["status"], "Failed")
        self.assertEqual(data["statusMessage"], "None of the identifier have match fields")

    def test_pptidentifier_not_found(self):
        """pptidentifier not configured in layout"""
        self.payload["configTables"].update({"layoutConfig": [{"fileName": "TEMP.MAINFRAME.FILE", "mfFieldName": ""}]})
        response = self.client.post(self.url, data=self.payload, content_type=content_type)
        data = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["status"], "Failed")
        self.assertEqual(data["statusMessage"], "Identifier doesn't match with File/Report")


class TestRedisFetch(TestCase):
    def setUp(self):
        self.url = "/sourceMatcher/fileVerification/"
        self.client = Client()
        self.payload = deepcopy(payload)

    @mock.patch.object(Session, "get")
    def test_redis_connect_failed(self, mock_get):
        """redis connect failed (wrong url)"""
        mock_get.side_effect = ConnectionError("mock connection")
        response = self.client.post(self.url, data=self.payload, content_type=content_type)
        data = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["status"], "Failed")
        self.assertEqual(data["statusMessage"], "Unable to connect Cache Storage")

    @mock.patch.object(Session, "get")
    def test_redis_not_ok(self, mock_get):
        """redis return `status_code != 200`"""
        mock_get_resp = Response(status=417)
        mock_get_resp.content = "mock content"
        mock_get.return_value = mock_get_resp
        response = self.client.post(self.url, data=self.payload, content_type=content_type)
        data = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["status"], "Failed")
        self.assertEqual(data["statusMessage"], "Unable to get File/Report")
