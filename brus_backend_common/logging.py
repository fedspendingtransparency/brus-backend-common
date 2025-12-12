import logging.config
import os
import threading

from time import sleep, time_ns
from typing import List, Tuple

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.urllib import URLLibInstrumentor
from opentelemetry.instrumentation.threading import ThreadingInstrumentor

from brus_backend_common.config import CONFIG


logger = logging.getLogger(__name__)


# Reasonable defaults to avoid clutter in our config files
DEFAULT_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": "[%(asctime)s] [%(levelname)s] - %(message)s", "datefmt": "%Y-%m-%d %H:%M:%S %Z"},
    },
    "handlers": {
        "console": {"formatter": "default", "class": "logging.StreamHandler"},
    },
    "loggers": {
        # i.e. "all modules"
        "": {"handlers": ["console"], "level": "INFO", "propagate": True},
        "__main__": {"level": "INFO", "propagate": True},  # for the __main__ module within scripts
    },
}


# CUSTOM Logging EXPORTER for debugging
class LoggingSpanProcessor(SpanProcessor):
    def __init__(self):
        self._lock = threading.Lock()
        self._flushed = True  # Simulates the state of flushing

    def on_end(self, span: ReadableSpan) -> None:
        trace_id = span.context.trace_id
        span_id = span.context.span_id
        logger.debug(f"Span ended: trace_id={trace_id}, span_id={span_id}, {span.name}_attributes={span.attributes}")

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Simulates flushing all spans within the given timeout."""
        start_time = time_ns()
        with self._lock:
            # Simulate some processing delay for flushing
            while not self._flushed:
                elapsed_time = (time_ns() - start_time) / 1_000_000  # Convert to milliseconds
                if elapsed_time > timeout_millis:
                    logger.warning("force_flush timed out.")
                    return False
                # Simulate work by sleeping briefly
                sleep(0.01)

        logger.debug("All spans flushed successfully.")
        return True


class CustomAttributeSpanProcessor(SpanProcessor):
    def __init__(self, attribute_key, attribute_value):
        self.attribute_key = attribute_key
        self.attribute_value = attribute_value

    def on_start(self, span, parent_context):
        # Add the custom attribute when the span starts
        span.set_attribute(self.attribute_key, self.attribute_value)


def add_custom_attribute_span_processors(tracerProvider: TracerProvider, attribute_pairs: List[Tuple[str, str]]):
    for key, value in attribute_pairs:
        custom_attribute_span_processor = CustomAttributeSpanProcessor(key, value)
        tracerProvider.add_span_processor(custom_attribute_span_processor)


def configure_logging(service_name: str = "brus_backend_common", **extra_attributes: str):
    logging.config.dictConfig(DEFAULT_CONFIG)

    resource = Resource.create(attributes={"service.name": service_name})
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    # Modify the following to add/remove information inside traces
    # The following will be added to every trace
    attribute_pairs = [
        ("TRACE_ENV", CONFIG.TRACE_ENV),
    ]
    if extra_attributes is not None:
        attribute_pairs = attribute_pairs + list(extra_attributes.items())
    # There are two TraceProvider's in OTLP and the static type checker struggles with it
    add_custom_attribute_span_processors(trace.get_tracer_provider(), attribute_pairs)  # type: ignore

    if CONFIG.IS_LOCAL:
        # if local, print the traces to the console
        trace.get_tracer_provider().add_span_processor(LoggingSpanProcessor())
        exporter = ConsoleSpanExporter()
    else:
        # Set up the OTLP exporter
        # Check out https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/
        # for more exporter configuration
        otel_endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        exporter = OTLPSpanExporter(endpoint=otel_endpoint) if otel_endpoint else None  # type: ignore
    if exporter is not None:
        # Type checker struggles with ConsoleSpanExporter and OTLPSpanExporter both being exporters
        trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(exporter))  # type: ignore

    otel_format = os.getenv("OTEL_PYTHON_LOG_FORMAT", "%(asctime)s %(levelname)s:%(name)s:%(message)s")
    LoggingInstrumentor(logging_format=otel_format)
    LoggingInstrumentor().instrument(tracer_provider=trace.get_tracer_provider(), set_logging_format=False)
    URLLibInstrumentor().instrument(tracer_provider=trace.get_tracer_provider())
    ThreadingInstrumentor().instrument()

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
