"""serializers"""
import json
from rest_framework import serializers

FILE_FORMATTER = "File Formatter"


def get_error_messages(field_name, field_type="str"):
    """return errors of particular to fields"""

    cmn_err_mssg = {
        "empty": field_name + " should not be empty",
        "invalid": field_name + " is invalid",
        "required": field_name + " is required",
        "null": field_name + " can't be null",
    }

    op_err_mssg = {
        "blank": field_name + " shouldn't be blank",
    }

    if field_type == "str":
        cmn_err_mssg.update(op_err_mssg)

    return cmn_err_mssg


def validate_ksd_file_details(value):
    error_msg = {
        "message": "ksdFileDetails are not proper",
        "pptidentifier": "pptidentifier field can't be empty",
        "fileName": "fileName field can't be empty",
        "fileType": "fileType field can't be empty",
        "pptidentifierType": "pptidentifierType field can't be empty",
    }

    if len(value) < 1:
        raise serializers.ValidationError(error_msg["message"])

    for val in value:
        field = json.loads(val)
        if field["pptidentifier"] is None or field["pptidentifier"].strip(" ") == "":
            raise serializers.ValidationError(error_msg["pptidentifier"])
        if field["fileName"] is None or field["fileName"].strip(" ") == "":
            raise serializers.ValidationError(error_msg["fileName"])
        if field["fileType"] is None or field["fileType"].strip(" ") == "":
            raise serializers.ValidationError(error_msg["fileType"])
        if field["pptidentifierType"] is None or field["pptidentifierType"].strip(" ") == "":
            raise serializers.ValidationError(error_msg["pptidentifierType"])

    return value


def validate_config_tables(value):
    """Config table errors"""
 
    import pandas as pd
 
    match_list = list()
    inq_list = list()
 
    if len(value.get("tbaMatchConfig", [])) > 0:
        match_config = pd.DataFrame(value["tbaMatchConfig"])
        match_config = match_config[match_config["matchType"].str.lower() == "compare with tba"]
        inq_config = pd.DataFrame(value["tbaInquiryConfig"])
        notice_config = pd.DataFrame(value["tbaNoticeInqConfig"])
 
        if not match_config.empty:
            match_list = match_config.inquiryDefName.tolist()
        if not inq_config.empty:
            inq_list += inq_config.inquiryDefName.tolist()
        if not notice_config.empty:
            inq_list += notice_config.inquiryDefName.tolist()
 
        for match_field in match_list:
            if match_field not in inq_list:
                raise serializers.ValidationError("Unable to match field(s) which are not inquired")
 
    return value


class ClientDetailsSerializer(serializers.Serializer):
    """Verify client details in request"""

    businessUnitClients = serializers.ListField(required=True)
    createdDate = serializers.CharField(required=True)
    clientName = serializers.CharField(required=True, error_messages=get_error_messages("Client name"))
    createdBy = serializers.CharField(required=True)
    clientCode = serializers.CharField(required=True, error_messages=get_error_messages("Client code"))
    id = serializers.IntegerField()


class ProcessJobMappingSerializer(serializers.Serializer):
    """verify client details dictionary"""

    eftSubject = serializers.CharField(required=True)
    jobName = serializers.CharField(required=True)
    businessUnitOps = serializers.DictField(required=True)
    clientDetails = ClientDetailsSerializer()
    process = serializers.DictField(required=True)
    createdDate = serializers.CharField(required=True)
    createdBy = serializers.CharField(required=True)
    ksdName = serializers.CharField(required=True)
    id = serializers.IntegerField(required=True)


class KsdConfigSerializer(serializers.Serializer):
    """"verify processJobMapping dictionary"""

    processJobMapping = ProcessJobMappingSerializer()
    ksdFileDetails = serializers.ListField(required=True, validators=[validate_ksd_file_details])


class RequestDetailSerializer(serializers.Serializer):
    """Validate request details dictionary"""

    uid = serializers.CharField(required=True, error_messages=get_error_messages("Request details uid"))
    userName = serializers.CharField(required=True, error_messages=get_error_messages("Request details userName"))
    phase = serializers.IntegerField(required=True, error_messages=get_error_messages("Request details phase", "int"))
    pluginName = serializers.CharField(required=True, error_messages=get_error_messages("Request details pluginName"))
    createTimeStamp = serializers.CharField(
        required=True, error_messages=get_error_messages("Request details createTimeStamp"),
    )


class ProcessFeatureConfig(serializers.Serializer):
    """"Validate process feature config dictionary"""

    businessUnitName = serializers.CharField(required=True, error_messages=get_error_messages("Business unit name"))
    phaseNames = serializers.CharField(required=True, error_messages=get_error_messages("Phase names"))
    processType = serializers.CharField(required=True, error_messages=get_error_messages("Process Type"))
    businessOpsName = serializers.CharField(required=True, error_messages=get_error_messages("Business ops name"))
    processName = serializers.CharField(required=True, error_messages=get_error_messages("Process name"))
    processJobMapping = serializers.DictField(required=True)

    def validate_phaseNames(self, value):
        error_msg = "phaseNames doesn't have SourceMatch"

        phase_name = json.loads(value)

        if "SourceMatch" not in phase_name.keys():
            raise serializers.ValidationError(error_msg)

        return value

    def validate_processJobMapping(self, value):
        error_msg = "processJobMapping format is invalid"

        if not isinstance(value["id"], int):
            raise serializers.ValidationError(error_msg)

        if value["jobName"] is None or value["jobName"].strip(" ") == "":
            raise serializers.ValidationError(error_msg)

        return value


class RulesConfigSerializer(serializers.Serializer):
    """Validate rulesConfig request details"""

    rulesDefinitions = serializers.ListField(required=True)


class TBAMatchConfigSerializer(serializers.Serializer):
    """Validate tbaMatchConfig request details"""

    id = serializers.IntegerField(required=True, error_messages=get_error_messages("Id in match", "int"))
    matchType = serializers.CharField(required=True)
    fileName = serializers.CharField(required=True, error_messages=get_error_messages("fileName in match"))
    sheetName = serializers.CharField(required=False, default="", allow_blank=True, allow_null=True)
    fileNameWoutSpace = serializers.CharField(
        required=True, error_messages=get_error_messages("fileNameWoutSpace in match")
    )
    sheetNameWoutSpace = serializers.CharField(required=False, default="", allow_blank=True, allow_null=True)
    mfFieldName = serializers.CharField(required=True, error_messages=get_error_messages("fieldname in match"))
    mfFieldWoutSpace = serializers.CharField(
        required=True, error_messages=get_error_messages("fieldwoutspace in match")
    )
    identifier = serializers.CharField(required=True, allow_null=True, allow_blank=True)
    # below fields are required for matchType's ("compare previous report", "compare with other report")
    fileNameDest = serializers.CharField(required=False, allow_null=True, allow_blank=True, default="")
    fileNameDestWoutSpace = serializers.CharField(required=False, allow_null=True, allow_blank=True, default="")
    sheetNameDest = serializers.CharField(required=False, allow_null=True, allow_blank=True, default="")
    sheetNameDestWoutSpace = serializers.CharField(required=False, allow_null=True, allow_blank=True, default="")
    mfFieldNameDest = serializers.CharField(required=False, allow_null=True, allow_blank=True, default="")
    mfFieldWoutSpaceDest = serializers.CharField(required=False, allow_null=True, allow_blank=True, default="")
    reportIdentifierDest = serializers.CharField(required=False, allow_null=True, allow_blank=True, default="")
    # below fields are required for matchType ("compare with tba")
    tbaFieldName = serializers.CharField(required=False, allow_null=True, allow_blank=True, default="")
    inquiryDefName = serializers.CharField(required=False, allow_null=True, allow_blank=True, default="")
    # below fields are common for all matchType's
    ruleName = serializers.CharField(trim_whitespace=False, required=True, allow_blank=True)
    actions = serializers.CharField(required=True, error_messages=get_error_messages("actions in match"))
    pptVerifyTba = serializers.CharField(required=False, allow_null=True, allow_blank=True, default="")

    def validate(self, value):
        if value["matchType"].lower() in ("compare previous report", "compare with other report",) and (
            value["fileNameDest"].strip() == ""
            or value["fileNameDestWoutSpace"].strip() == ""
            or value["mfFieldNameDest"].strip() == ""
            or value["mfFieldWoutSpaceDest"].strip() == ""
        ):
            raise serializers.ValidationError("Dest file or mfField name(s) cannot be empty")
        elif value["matchType"].lower() in ("compare with tba",) and (
            value["tbaFieldName"].strip() == "" or value["inquiryDefName"].strip() == ""
        ):
            raise serializers.ValidationError("tba field or inquiry def name cannot be empty")

        return value

    def validate_identifier(self, value):
        if value is None or value == "null":
            return ""
        return value


class TBAInquiryConfigSerializer(serializers.Serializer):
    """Validate tbaInquiryConfig request details"""

    # subJsonKey,metadata,rowMatrix,columnMatrix changed to - allowed to be null
    id = serializers.IntegerField(required=True)
    inquiryName = serializers.CharField(required=True, error_messages=get_error_messages("inquiryname in inquiry"))
    parNM = serializers.CharField(required=True, allow_null=True)
    panelId = serializers.IntegerField(required=True, error_messages=get_error_messages("panelId in inquiry"))
    tbaFieldName = serializers.CharField(required=True, error_messages=get_error_messages("tbafieldname in inquiry"))
    fieldType = serializers.CharField(required=False)
    jsonKey = serializers.CharField(required=True, error_messages=get_error_messages("jsonkey in inquiry"))
    subJsonKey = serializers.CharField(required=False, allow_null=True, allow_blank=True, default="")
    metaData = serializers.CharField(required=False, allow_null=True, allow_blank=True, default="")
    identifier = serializers.CharField(required=True, allow_blank=True)
    recordIdentifier = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    inquiryDefName = serializers.CharField(
        required=True, error_messages=get_error_messages("inquirydefname in inquiry")
    )
    sequence = serializers.CharField(required=True)
    effDateType = serializers.CharField(required=True)
    effFromDate = serializers.CharField(required=True)
    effToDate = serializers.CharField(required=True)
    rowMatrix = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    columnMatrix = serializers.CharField(required=False, allow_null=True, allow_blank=True)


class TBAEventHistInqConfigSerializer(serializers.Serializer):
    """Validate tbaEventHistoryConfig request details"""

    eventHistDefName = serializers.CharField(required=True)
    effFromDate = serializers.CharField(required=True)
    effToDate = serializers.CharField(required=True)
    eventName = serializers.CharField(required=True)
    actLongDesc = serializers.CharField(required=True)
    tbaFiledName = serializers.CharField(required=True, allow_null=True)
    jsonKey = serializers.CharField(required=True, allow_null=True)


class TBANoticeInqConfigSerializer(serializers.Serializer):
    """Validate tbaNoticesConfig request details"""

    noticeName = serializers.CharField(required=True, error_messages=get_error_messages("noticename"))
    noticeId = serializers.IntegerField(required=True, error_messages=get_error_messages("noticeId"))
    inquiryDefName = serializers.CharField(required=True, error_messages=get_error_messages("inquirydefname in notice"))
    tba_field_name = serializers.CharField(required=True, error_messages=get_error_messages("tbafieldname in notice"))
    jsonKey = serializers.CharField(required=True, error_messages=get_error_messages("jsonkey in notice"))
    subJsonKey = serializers.CharField(required=False, allow_blank=True, default="")
    metadata = serializers.CharField(required=False, allow_blank=True, default="")
    identifier = serializers.CharField(required=True, allow_blank=True)


class TBAPendEventInqConfigSerializer(serializers.Serializer):
    """validate tbaPendEventInqConfig request details"""

    pendgEvntDefName = serializers.CharField(required=True, error_messages=get_error_messages("event def name"))
    eventName = serializers.CharField(required=True, error_messages=get_error_messages("event name"))
    eventLongDesc = serializers.CharField(required=True, error_messages=get_error_messages("event long desc"))
    jsonKey = serializers.CharField(required=True, error_messages=get_error_messages("json key in event"))


class ConfigTableSerializer(serializers.Serializer):
    """Validate config tables dictionary"""

    rulesConfig = serializers.ListField(
        child=RulesConfigSerializer(), required=False, allow_null=True, allow_empty=True, default=[]
    )
    tbaMatchConfig = serializers.ListField(
        child=TBAMatchConfigSerializer(),
        required=False,
        allow_null=True,
        allow_empty=True,
        error_messages=get_error_messages("Match Config", "list"),
        default=[],
    )
    tbaInquiryConfig = serializers.ListField(
        child=TBAInquiryConfigSerializer(), required=False, allow_null=True, allow_empty=True, default=[]
    )
    tbaNoticeInqConfig = serializers.ListField(
        child=TBANoticeInqConfigSerializer(), required=False, allow_null=True, allow_empty=True, default=[]
    )
    tbaEventHistInqConfig = serializers.ListField(
        child=TBAEventHistInqConfigSerializer(), required=False, allow_null=True, allow_empty=True, default=[]
    )
    tbaPendingEventInqConfig = serializers.ListField(
        child=TBAPendEventInqConfigSerializer(), required=False, allow_null=True, allow_empty=True, default=[]
    )
    ksdOutputFileDetails = serializers.ListField(required=False, allow_empty=True, default=[])
    layoutConfig = serializers.ListField(allow_empty=False)


class ValidateRequestSerializer(serializers.Serializer):
    """Verify Request from Orchestrator"""

    ksdConfig = KsdConfigSerializer()
    botOutput = serializers.DictField(required=True)
    requestDetails = RequestDetailSerializer()
    processFeatureConfig = ProcessFeatureConfig()
    configTables = ConfigTableSerializer()  # validators=[validate_config_tables])
    redisKeys = serializers.DictField(required=False)
