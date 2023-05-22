import functools
import requests

from py_zipkin.transport import BaseTransportHandler
from py_zipkin.zipkin import ZipkinAttrs, zipkin_span

from TBASourceMatcherV2.settings import ZIPKIN_URL, ZIPKIN_SPAN, ZIPKIN_SAMPLE_RATE

zipkin_span_port = ZIPKIN_SPAN
zipkin_span_sample_rate = ZIPKIN_SAMPLE_RATE


class HttpTransport(BaseTransportHandler):
    """Override the default handle for Zipkin."""

    def get_max_payload_bytes(self):
        return None

    def send(self, encoded_span):
        # print("Sending Payload to Zipkin.")
        # try:
        #     requests.post(url=ZIPKIN_URL,
        #                   data=encoded_span,
        #                   headers={'Content-Type': 'application/x-thrift'},
        #                   )
        # except Exception:
        #     pass
        pass


def zipkin_custom_span(func):
    @functools.wraps(func)
    def wrapper_method(self, request, *args, **kwargs):

        with zipkin_span(
            service_name="Rservice",
            zipkin_attrs=ZipkinAttrs(
                trace_id=request.headers.get("X-B3-TraceID"),
                span_id=request.headers.get("X-B3-SpanID"),
                parent_span_id=request.headers.get("X-B3-ParentSpanID"),
                flags=request.headers.get("X-B3-Flags"),
                is_sampled=request.headers.get("X-B3-Sampled"),
            ),
            span_name="index_service1",
            transport_handler=HttpTransport().send,
            port=zipkin_span_port,
            sample_rate=zipkin_span_sample_rate,  # Value between 0 and 100.0
        ):
            value = func(self, request)
            # print("ValueResp:", value)
        return value

    return wrapper_method
