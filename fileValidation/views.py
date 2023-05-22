"""Views"""
import json
import socket
from django.conf import settings
from django.http import JsonResponse
from rest_framework.views import APIView
from rest_framework.parsers import JSONParser
from .utils import SourceMatch
from .helpers import LOGGER, ConfigError
from .serializers import ValidateRequestSerializer
from utilities.zipkinDecorator import zipkin_custom_span


class Processing(APIView):
    """Processing is the class responsible for
    processing orchestrator request"""

    parser_classes = (JSONParser,)

    @zipkin_custom_span
    def post(self, request):
        """Fetching required fields sent through post"""
        header_details = dict(
            trace_id=request.headers.get("X-B3-TraceID", ""),
            span_id=request.headers.get("X-B3-SpanID", ""),
            parent_span_id=request.headers.get("X-B3-ParentSpanID", ""),
            flags=request.headers.get("X-B3-Flags", ""),
            is_sampled=request.headers.get("X-B3-Sampled", ""),
            serverName=str(socket.gethostname()),
        )

        request_data = dict()
        serialized_request = ValidateRequestSerializer(data=request.data)
        try:
            if serialized_request.is_valid():
                serialized_data = serialized_request.validated_data
                request_data["ksdConfig"] = request.data["ksdConfig"] # For excel formatter
                request_data["processJobMapping"] = serialized_data["ksdConfig"]["processJobMapping"]
                request_data["clientId"] = serialized_data["ksdConfig"]["processJobMapping"]["clientDetails"]["clientCode"]
                request_data["clientName"] = serialized_data["ksdConfig"]["processJobMapping"]["clientDetails"]["clientName"]
                request_data["botOutput"] = serialized_data["botOutput"] # For excel formatter
                request_data["ksdFileDetails"] = serialized_data["ksdConfig"]["ksdFileDetails"]
                botoutput = serialized_data.get("botOutput")
                request_data["File Formatter"] = botoutput.get("File Formatter", {})
                request_data["File Validator"] = botoutput.get("File Validator", {})
                request_data["uid"] = serialized_data["requestDetails"]["uid"]
                request_data["userName"] = serialized_data["requestDetails"]["userName"]
                request_data["phaseId"] = serialized_data["requestDetails"]["phase"]
                request_data["pluginName"] = serialized_data["requestDetails"]["pluginName"]
                request_data["createTimeStamp"] = serialized_data["requestDetails"]["createTimeStamp"]
                request_data["applicationType"] = serialized_data["requestDetails"]["pluginName"]

                request_data["processFeatureConfig"] = request.data["processFeatureConfig"] # For excel formatter
                request_data["phaseNames"] = serialized_data["processFeatureConfig"]["phaseNames"]
                request_data["businessOps"] = serialized_data["processFeatureConfig"]["businessOpsName"]
                request_data["processType"] = serialized_data["processFeatureConfig"]["processType"]
                request_data["processName"] = serialized_data["processFeatureConfig"]["processName"]
                request_data["businessUnit"] = serialized_data["processFeatureConfig"]["businessUnitName"]
                request_data["jobName"] = serialized_data["processFeatureConfig"]["processJobMapping"]["jobName"]
                request_data["pjmId"] = serialized_data["processFeatureConfig"]["processJobMapping"]["id"]

                # request_data['secretEngine'] = ??

                request_data["rulesConfig"] = request.data["configTables"]["rulesConfig"]
                request_data["tbaUpdateConfig"] = request.data["configTables"]["tbaUpdateConfig"]
                request_data["tbaMatchConfig"] = serialized_data["configTables"]["tbaMatchConfig"]
                request_data["tbaInquiryConfig"] = serialized_data["configTables"]["tbaInquiryConfig"]
                request_data["tbaEventHistInqConfig"] = serialized_data["configTables"]["tbaEventHistInqConfig"]
                request_data["tbaNoticeInqConfig"] = serialized_data["configTables"]["tbaNoticeInqConfig"]
                request_data["tbaPendEventInqConfig"] = list()
                if "tbaPendingEventInqConfig" in serialized_data["configTables"].keys():
                    request_data["tbaPendEventInqConfig"] = serialized_data["configTables"]["tbaPendingEventInqConfig"]
                request_data["ksdOutputFileDetails"] = serialized_data["configTables"]["ksdOutputFileDetails"]
                request_data["layoutConfig"] = serialized_data["configTables"]["layoutConfig"]
                request_data["redisKeys"] = serialized_data.get("redisKeys", {})
            else:
                raise ConfigError(serialized_request.errors)

        except ConfigError as err:
            uid = request_data.get("uid", "")
            time_stamp = request_data.get("createTimeStamp", "")
            LOGGER.error(
                f"Validation error occured for request_id {uid}, time_stamp {time_stamp}, Error:\n {err}",
                extra=header_details,
            )
            message = "Some important fields are missing"

            return JsonResponse({"status": "Failed", "statusMessage": message,}, status=400,)

        uid = request_data["uid"]
        time_stamp = request_data["createTimeStamp"]
        LOGGER.info(
            f"Validation success! Processing request_id {uid}, time_stamp {time_stamp}", extra=header_details,
        )
        source_match = SourceMatch(request_data, header_details=header_details)

        return JsonResponse(source_match.get_response(), status=200)
